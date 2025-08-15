import socket
import json

class UDPClient:
    def __init__(self, timeout=2.0):
        self.timeout = timeout

    def connect(self, host, port=4500):
        self.host = host
        self.port = port

    def send_request(self, command, database, storage, query='', value=None):
        req = {
            "command": command,
            "database": database,
            "storage": storage,
            "query": query,
            "value": value
        }
        message = json.dumps(req).encode()

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(self.timeout)
            sock.sendto(message, (self.host, self.port))

            try:
                response, _ = sock.recvfrom(65536)
                return json.loads(response.decode('utf-8'))
            except socket.timeout:
                print("No response received (timeout)")
                return None
