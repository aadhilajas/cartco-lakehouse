"""Read-only synthetic SSH/SFTP server, rooted at one explicitly supplied folder."""
import hmac
import os
from pathlib import Path, PurePosixPath
import socket
from threading import Event, Thread

import paramiko


class Authentication(paramiko.ServerInterface):
    def __init__(self, username, password):
        self.username, self.password = username, password

    def check_auth_password(self, username, password):
        return paramiko.AUTH_SUCCESSFUL if hmac.compare_digest(username, self.username) and hmac.compare_digest(password, self.password) else paramiko.AUTH_FAILED

    def get_allowed_auths(self, username):
        return "password"

    def check_channel_request(self, kind, channel_id):
        return paramiko.OPEN_SUCCEEDED if kind == "session" else paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED


class ReadOnlyFiles(paramiko.SFTPServerInterface):
    def __init__(self, server, *, root):
        super().__init__(server)
        self.root = Path(root).resolve()

    def local(self, path):
        if "\\" in path or ".." in PurePosixPath(path).parts:
            raise PermissionError("Unsafe SFTP path")
        result = (self.root / path.lstrip("/")).resolve()
        if not result.is_relative_to(self.root):
            raise PermissionError("SFTP path leaves fixture root")
        return result

    def stat(self, path):
        try:
            return paramiko.SFTPAttributes.from_stat(self.local(path).stat())
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno or 13)

    lstat = stat

    def open(self, path, flags, attr):
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
            return paramiko.SFTP_PERMISSION_DENIED
        try:
            handle = paramiko.SFTPHandle(flags)
            handle.readfile = self.local(path).open("rb")
            return handle
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno or 13)


class SFTPFixture:
    def __init__(self, root, *, host="127.0.0.1", port=0, username="fixture", password="fixture-local-only", key=None):
        self.root, self.username, self.password = root, username, password
        self.key = key or paramiko.RSAKey.generate(2048)
        self.stop = Event()
        self.socket = socket.socket()
        self.socket.bind((host, port))
        self.socket.listen(5)
        self.socket.settimeout(0.25)
        self.host, self.port = self.socket.getsockname()
        self.connections = []
        self.thread = Thread(target=self.serve, daemon=True)

    def known_hosts(self, hostname=None):
        return f"[{hostname or self.host}]:{self.port} {self.key.get_name()} {self.key.get_base64()}\n"

    def serve(self):
        while not self.stop.is_set():
            try:
                client, _ = self.socket.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            transport = paramiko.Transport(client)
            self.connections.append(transport)
            transport.add_server_key(self.key)
            transport.set_subsystem_handler("sftp", paramiko.SFTPServer, ReadOnlyFiles, root=self.root)
            try:
                transport.start_server(server=Authentication(self.username, self.password))
            except (paramiko.SSHException, EOFError):
                transport.close()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.socket.close()
        for connection in self.connections:
            connection.close()
        self.thread.join(timeout=3)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=12222)
    parser.add_argument("--key", type=Path, required=True)
    parser.add_argument("--known-hosts", type=Path, required=True)
    parser.add_argument("--known-hostname")
    args = parser.parse_args()
    args.key.parent.mkdir(parents=True, exist_ok=True)
    if args.key.exists():
        key = paramiko.RSAKey.from_private_key_file(str(args.key))
    else:
        key = paramiko.RSAKey.generate(2048)
        key.write_private_key_file(str(args.key))
    with SFTPFixture(args.root, host=args.host, port=args.port, key=key) as server:
        args.known_hosts.write_text(server.known_hosts(args.known_hostname), encoding="utf-8")
        try:
            server.stop.wait()
        except KeyboardInterrupt:
            pass
