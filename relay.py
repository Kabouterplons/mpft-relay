#!/usr/bin/env python3
"""
MP FlightTunes — Relay Server v2.0
Fly.io deployment

Handshake protocol (eerste bytes na TCP connect):
  [role : uint8]         0x01 = Sender, 0x02 = Receiver
  [room : UTF-8 + \0]    bijv. "gatw-group\0"
Daarna stromen normale FTNS pakketten.
"""

import socket
import threading
import select
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("relay")

HOST    = "0.0.0.0"
PORT    = int(os.environ.get("PORT", 12345))
BUFFER  = 65536
TIMEOUT = 60  # seconden zonder data -> verbinding verbreken

ROLE_SENDER   = 0x01
ROLE_RECEIVER = 0x02

# rooms[room_name] = { addr: socket }
rooms      = {}
rooms_lock = threading.Lock()


def remove_client(room, addr, sock):
    try:
        sock.close()
    except Exception:
        pass
    with rooms_lock:
        if room in rooms:
            rooms[room].pop(addr, None)
            remaining = len(rooms[room])
            if not rooms[room]:
                del rooms[room]
        else:
            remaining = 0
    log.info("[%s] vertrokken: %s  (%d over)", room, addr, remaining)


def handle_client(sock, addr, room, role):
    role_name = "Sender" if role == ROLE_SENDER else "Receiver"
    log.info("[%s] verbonden: %s (%s)", room, addr, role_name)

    with rooms_lock:
        if room not in rooms:
            rooms[room] = {}
        rooms[room][addr] = sock

    try:
        while True:
            ready, _, _ = select.select([sock], [], [], TIMEOUT)
            if not ready:
                log.info("[%s] timeout: %s", room, addr)
                break

            data = sock.recv(BUFFER)
            if not data:
                break

            with rooms_lock:
                peers = {a: s for a, s in rooms.get(room, {}).items()
                         if a != addr}

            dead = []
            for peer_addr, peer_sock in peers.items():
                try:
                    peer_sock.sendall(data)
                except Exception:
                    dead.append((peer_addr, peer_sock))

            for peer_addr, peer_sock in dead:
                log.info("[%s] dode peer verwijderd: %s", room, peer_addr)
                remove_client(room, peer_addr, peer_sock)

    except Exception as e:
        log.warning("[%s] fout bij %s: %s", room, addr, e)
    finally:
        remove_client(room, addr, sock)


def read_handshake(sock):
    buf = b""
    sock.settimeout(5.0)
    try:
        while b"\x00" not in buf:
            chunk = sock.recv(64)
            if not chunk:
                return None, None
            buf += chunk
        sock.settimeout(None)
        role = buf[0]
        null_pos = buf.index(b"\x00")
        room = buf[1:null_pos].decode("utf-8", errors="ignore").strip()
        if not room:
            room = "default"
        return role, room
    except Exception as e:
        log.warning("Handshake fout: %s", e)
        sock.settimeout(None)
        return None, None


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    srv.bind((HOST, PORT))
    srv.listen(20)
    log.info("MP FlightTunes relay luistert op poort %d", PORT)

    while True:
        try:
            conn, (ip, port) = srv.accept()
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            addr = "%s:%d" % (ip, port)
            role, room = read_handshake(conn)

            if role is None:
                log.warning("Ongeldige handshake van %s, gesloten", addr)
                conn.close()
                continue

            t = threading.Thread(
                target=handle_client,
                args=(conn, addr, room, role),
                daemon=True
            )
            t.start()

        except Exception as e:
            log.error("Accept fout: %s", e)


if __name__ == "__main__":
    main()
