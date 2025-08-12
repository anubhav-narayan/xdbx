import asyncio
from .handler import handler
from .config import HOST, PORT, BUFFER_SIZE

class UDPServerProtocol:
    def __init__(self, store: dict):
        self.store = store

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        response = handler(data, self.store)
        self.transport.sendto(response, addr)

async def start_server(store):
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol({}),
        local_addr=(HOST, PORT)
    )
    print(f"HPC UDP server running on {HOST}:{PORT}")
    try:
        await asyncio.Future()  # run forever
    finally:
        transport.close()
