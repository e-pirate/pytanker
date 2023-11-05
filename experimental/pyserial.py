#!/usr/bin/env python3.11

import sys
import logging
import logging.handlers
import time
import os
import asyncio
import serial
#import serial_asyncio
import functools
import signal
import random
from datetime import datetime, timedelta

_version_ = "0.0.1"

jobs = { 'light': { 'duration': 1 }, 'co2': { 'duration': 1.5 }, 'ferts': { 'duration': 2 }, 'pump': { 'duration': 4 } }
statedb = { 'light': { 'pending': False }, 'co2': { 'pending': False }, 'ferts': { 'pending': False }, 'pump': { 'pending': False } }


def handler_shutdown(signame: str):
    log = logging.getLogger("__main__")
    log.info(f"Received {signame}: exiting..")

    for t in asyncio.all_tasks():                                                                       # Cancel the dispatcher loop, pending aftercheck
        if t._coro.__name__ in ['serial_loop', 'async_serial_sender', 'dummy_send']:                                                           # will be cancelled automatically
            t.cancel()


async def async_serial_sender(ser):
    log = logging.getLogger("__main__")

    while True:
        try:
            await ser.tx_flag.wait()
            try:
                ser.write(ser.tx_buff.encode("ascii"))
            except Exception as e:
                log.error(e)
            else:
                ser.tx_buff = ''
                ser.tx_flag.clear()
        except asyncio.CancelledError:
            log.info("Shutting down async serial sender")
            break


def serial_send(ser, data):
    ser.tx_buff += data
    ser.tx_flag.set()


async def serial_loop(ser):
    log = logging.getLogger("__main__")
    ser.tx_flag = asyncio.Event()
    ser.tx_buff = ''

    log.info("Entering serial loop..")
    try:
        await async_serial_sender(ser)
    except asyncio.CancelledError:
        log.info("Shutting down serial loop")


#    """ Gracefull shutdown """
#    pending_tasks = []
#    for t in asyncio.all_tasks():
#        match t._coro.__name__:
#            case 'tasks_aftercheck':                                                                    # Cancel pending aftercheck ASAP
#                t.cancel()
#            case 'task_check':                                                                          # Collect active check tasks
#                pending_tasks.append(t)
#    if pending_tasks:
#        try:
#            await asyncio.shield(tasks_stopwait(pending_tasks, timeout=1))                              # Try to wait for tasks to finish during timeout seconds
#        except asyncio.CancelledError:
#            log.warning("Graceful shutdown terminated, cancelling pending tasks")

async def dummy_send(ser):
    log = logging.getLogger("__main__")
    while True:
        data = ''.join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(8))
        log.debug(data)
        serial_send(ser, data + '\r\n')
        try:
            await asyncio.sleep(1)                                     # Schedule check for the next round upcoming second
        except asyncio.CancelledError:
            log.info("Shutting down dummy sender loop")
            break



async def main_loop(jobs: dict):
    """Add signal handlers"""
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(getattr(signal, 'SIGINT'), functools.partial(handler_shutdown, 'SIGINT'))
    loop.add_signal_handler(getattr(signal, 'SIGTERM'), functools.partial(handler_shutdown, 'SIGTERM'))

    #TODO: use aioserial 1.3.1
    try:
        ser = serial.serial_for_url('/dev/pts/7', baudrate=115200, write_timeout=0)                     # write_timeout mast be set to 0 to make writes non-blocking
    except Exception as e:
        log.critical(f"Failed to open serial device: {e}")
        sys.exit(1)

    await asyncio.gather(serial_loop(ser=ser), dummy_send(ser))

    ser.close()


def main():
    """ Setup logging """
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt='%(asctime)s.%(msecs)03d asynctest: (%(levelname).1s) %(message)s', datefmt="%H:%M:%S"))
    log = logging.getLogger(__name__)
    log.handlers.clear()
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    log.info(f"Starting serial-sender test program v{_version_}..")

    asyncio.run(main_loop(jobs=jobs))

    log.info(f"Shutting down serial test program v{_version_}..")


if __name__ == "__main__":
    main()

"""Serial debug usage:
socat -d -d pty,raw,echo=1,b115200 pty,raw,echo=1,b115200
cu -h -s 115200 -l /dev/pts/9
"""

