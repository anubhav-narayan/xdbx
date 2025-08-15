import asyncio
from socket import AF_UNIX
from .handler import handler
import signal
import logging

HOST = '0.0.0.0'
PORT = 4500

class UDPServerProtocol(asyncio.Protocol):
    def __init__(self, store: dict):
        self.store = store
        self.log = logging.getLogger('UDP Server')
        self.log.setLevel(logging.DEBUG)

    def connection_made(self, transport):
        self.log.info(f"Connected!")
        self.transport = transport

    def datagram_received(self, data, addr):
        from traceback import TracebackException
        self.log.info(f"Address: {addr[0]}:{addr[1]}, Request: {data.decode('utf-8')}")
        try:
            response = handler(data, self.store)
            self.transport.sendto(response, addr)
        except Exception as e:
            e = TracebackException.from_exception(e)
            msg = "\n".join([x for x in e.format()])
            self.log.error(f'{msg}')

    def connection_lost(self, exc):
        self.log.info("UDP connection closed")

    def cleanup(self):
        # Perform any necessary cleanup here
        self.log.info("Cleaning up store...")
        for x in self.store:
            self.store[x].close()
        self.log.info("Goodbye!")

async def start_server():
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    def shutdown():
        stop_event.set()
    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol({}),
        local_addr=(HOST, PORT)
    )
    print(f"HPC UDP server running on {HOST}:{PORT}")
    print("Use Ctrl+C to exit")
    try:
        await stop_event.wait()  # run forever
    finally:
        protocol.cleanup()
        transport.close()
