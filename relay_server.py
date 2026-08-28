#!/usr/bin/env python3
"""
Nakuya Relay Server - Deploy on any public VPS
Usage: python relay_server.py [port]
Default port: 4080

Free VPS options:
- Oracle Cloud Free Tier (always free)
- Render.com (free tier)
- Railway.app (free tier)
- PythonAnywhere (free tier)
"""

import socket
import struct
import threading
import time
import sys
import json

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 4080

# Registry of connected peers
# Each connection registers as either "controller" or "client_<id>"
peers = {}
peers_lock = threading.Lock()
# Map client_id -> (controller_conn, client_conn) pairs
pairs = {}
pairs_lock = threading.Lock()

def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        try:
            chunk = conn.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        except (ConnectionResetError, BrokenPipeError, OSError):
            return None
    return buf

def recv_frame(conn):
    hdr = recv_exact(conn, 4)
    if not hdr:
        return None
    length = struct.unpack(">I", hdr)[0]
    if length > 50 * 1024 * 1024:
        return None
    data = recv_exact(conn, length)
    return data

def send_frame(conn, data):
    try:
        conn.sendall(struct.pack(">I", len(data)) + data)
    except (ConnectionResetError, BrokenPipeError, OSError):
        pass

def pipe(src, dst, name):
    """Forward all frames from src to dst until disconnect."""
    try:
        while True:
            data = recv_frame(src)
            if data is None:
                break
            send_frame(dst, data)
    except Exception:
        pass
    finally:
        log(f"[*] Pipe {name} ended")
        try:
            src.close()
        except Exception:
            pass
        try:
            dst.close()
        except Exception:
            pass

def handle_connection(conn, addr):
    """Handle incoming connection. First frame determines role."""
    conn.settimeout(10)
    try:
        # First message must be a registration JSON
        reg_data = recv_frame(conn)
        if not reg_data:
            conn.close()
            return

        reg = json.loads(reg_data.decode())
        role = reg.get("role", "")
        peer_id = reg.get("id", "")

        conn.settimeout(None)

        if role == "controller":
            log(f"[+] Controller connected from {addr[0]}:{addr[1]}")
            with peers_lock:
                peers["controller"] = conn

            # Wait for a client to pair
            while True:
                time.sleep(1)
                with pairs_lock:
                    if "active_pair" in pairs:
                        client_conn = pairs["active_pair"]
                        break
                # Check if controller is still alive
                try:
                    conn.settimeout(1)
                    test = conn.recv(1, socket.MSG_PEEK)
                    if not test:
                        break
                    conn.settimeout(None)
                except socket.timeout:
                    continue
                except Exception:
                    break

            with pairs_lock:
                pairs.pop("active_pair", None)

            log(f"[+] Pairing controller with client")

            # Send pairing confirmation to both sides
            confirm = json.dumps({"status": "paired"}).encode()
            send_frame(conn, confirm)
            send_frame(client_conn, confirm)

            # Now pipe data bidirectionally
            t1 = threading.Thread(target=pipe, args=(conn, client_conn, "ctrl->client"), daemon=True)
            t2 = threading.Thread(target=pipe, args=(client_conn, conn, "client->ctrl"), daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            with peers_lock:
                peers.pop("controller", None)
            log("[*] Controller disconnected")

        elif role == "client":
            log(f"[+] Client connected from {addr[0]}:{addr[1]} id={peer_id}")
            with peers_lock:
                peers[f"client_{peer_id}"] = conn

            # Check if controller is waiting
            with peers_lock:
                controller = peers.get("controller")

            if controller:
                with pairs_lock:
                    pairs["active_pair"] = conn
                log(f"[+] Client queued for pairing")
                # Keep connection alive until paired
                while True:
                    time.sleep(1)
                    with pairs_lock:
                        if "active_pair" not in pairs or pairs.get("active_pair") != conn:
                            break
                    try:
                        conn.settimeout(1)
                        test = conn.recv(1, socket.MSG_PEEK)
                        if not test:
                            break
                        conn.settimeout(None)
                    except socket.timeout:
                        continue
                    except Exception:
                        break
            else:
                # No controller yet - wait for one
                log(f"[*] No controller yet, waiting...")
                while True:
                    time.sleep(1)
                    with peers_lock:
                        controller = peers.get("controller")
                    if controller:
                        with pairs_lock:
                            pairs["active_pair"] = conn
                        break
                    try:
                        conn.settimeout(1)
                        test = conn.recv(1, socket.MSG_PEEK)
                        if not test:
                            break
                        conn.settimeout(None)
                    except socket.timeout:
                        continue
                    except Exception:
                        break

            with peers_lock:
                peers.pop(f"client_{peer_id}", None)
            log(f"[*] Client {peer_id} disconnected")

        else:
            log(f"[-] Unknown role: {role} from {addr[0]}")
            conn.close()

    except Exception as e:
        log(f"[-] Error handling {addr[0]}: {e}")
        try:
            conn.close()
        except Exception:
            pass

def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", PORT))
    srv.listen(20)
    log(f"[+] Nakuya Relay listening on 0.0.0.0:{PORT}")
    log(f"[*] Waiting for controller and client connections...")

    while True:
        try:
            conn, addr = srv.accept()
            threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            break
        except Exception as e:
            log(f"[-] Accept error: {e}")

    srv.close()
    log("[*] Relay stopped")

if __name__ == "__main__":
    main()