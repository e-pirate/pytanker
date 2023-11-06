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
        if t._coro.__name__ in ['serial_loop', 'dummy_write']:                                                           # will be cancelled automatically
            t.cancel()


async def async_serial_reader(ser):
    log = logging.getLogger("__main__")

    while True:
        if ser.in_waiting > 0:
            async with ser.rx_lock:
                ser.rx_buff += ser.read(ser.in_waiting).decode("ascii")
        try:
            await asyncio.sleep(0.1)
        except:
            log.info("Shutting down async serial reader")
            break


async def serial_read(ser):
    async with ser.rx_lock:
        data = ser.rx_buff
        ser.rx_buff = ''
    return(data)


async def async_serial_writer(ser):
    log = logging.getLogger("__main__")

    while True:
        try:
            await ser.tx_flag.wait()
        except asyncio.CancelledError:
            log.info("Shutting down async serial writer")
            break
        else:
            async with ser.tx_lock:
                try:
                    ser.write(ser.tx_buff.encode("ascii"))
                except Exception as e:
                    log.error(f"Failed to send data over serial: {e}")
                else:
                    ser.tx_buff = ''
                    ser.tx_flag.clear()


async def serial_write(ser, data):
    async with ser.tx_lock:
        ser.tx_buff += data
        ser.tx_flag.set()


async def serial_loop(ser):
    log = logging.getLogger("__main__")
    ser.tx_flag = asyncio.Event()
    ser.tx_lock = asyncio.Lock()
    ser.tx_buff = ''
    ser.rx_flag = asyncio.Event()
    ser.rx_lock = asyncio.Lock()
    ser.rx_buff = ''

    log.info("Entering serial loop..")
    try:
        await asyncio.gather(async_serial_reader(ser), async_serial_writer(ser))
    except asyncio.CancelledError:
        log.info("Shutting down serial loop")

    """ Gracefull shutdown """
    for _ in asyncio.all_tasks():
        if _._coro.__name__ in ['async_serial_reader', 'async_serial_writer']:
            _.cancel()


async def dummy_write(ser):
    log = logging.getLogger("__main__")
    while True:
        #        data = ''.join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(8))
#        log.debug(data)
#        await serial_write(ser, data + '\r\n')
        data = await serial_read(ser)
        if len(data):
            log.info(f"{data}")
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

    await asyncio.gather(serial_loop(ser=ser), dummy_write(ser))

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

