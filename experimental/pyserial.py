#!/usr/bin/env python3.11

import sys
import logging
import logging.handlers
import time
import os
import asyncio
import concurrent.futures
import serial
#import serial_asyncio
import functools
import signal
import random
from datetime import datetime, timedelta
#import threading

_version_ = "0.0.1"


def handler_shutdown(signame: str):
    log = logging.getLogger("__main__")
    log.info(f"Received {signame}: exiting..")

    for t in asyncio.all_tasks():                                                                       # Cancel the dispatcher loop, pending aftercheck
        if t._coro.__name__ in ['serial_loop', 'terminal']:                                             # will be cancelled automatically
            t.cancel()


def serial_write(ser: serial.Serial, data: str):
    _serial_check(ser)

    ser._write_buff += data
    ser._write_flag.set()


async def serial_write_async(ser: serial.Serial, data: str):
    _serial_check(ser)

    ser._write_buff += data
    ser._write_flag.set()

    await ser._write_flag.wait()


async def _serial_async_writer(ser: serial.Serial):
    log = logging.getLogger("__main__")

    while ser.is_open:
        try:
            await ser._write_flag.wait()
        except asyncio.CancelledError:
            log.debug("Shutting down asynchronous serial writer")
            break
        else:
            try:
                ser.write(ser._write_buff)
            except serial.serialutil.SerialException as e:
                log.error(f"Serial connection failed, aborting writer")
                ser.write_exception = e
                raise
            else:
                ser._write_buff = b''
                ser._write_flag.clear()

    log.debug("Asynchronous serial writer is down")


async def _serial_async_reader(ser: serial.Serial):
    log = logging.getLogger("__main__")
    loop = asyncio.get_running_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    while ser.is_open:
        try:
            data = await loop.run_in_executor(executor, ser.read)                                       # Execute blocking read() in thread and wait for the first byte
            if ser.in_waiting:                                                                          # check if there is more data present in receive buffer
                data += ser.read(ser.in_waiting)                                                        # read all available data at a time, append to the local buffer
        except serial.serialutil.SerialException as e:
            log.error("Serial connection failed, aborting reader")
            executor.shutdown()
            ser.read_exception = e
            ser._read_flag.set()
            raise
        except asyncio.CancelledError as e:
            log.debug("Shutting down asynchronous serial reader")
            ser.read_exception = e
            break
        else:
            ser._read_buff += data
            ser._read_flag.set()

    ser.cancel_read()                                                                                   # Release blocked executor by cancelling read()
    executor.shutdown()
    ser._read_flag.set()
    log.debug("Asynchronous serial reader is down")


def _serial_check(ser: serial.Serial):
    try:
        if not ser.is_open:
            raise RuntimeError("Serial connection is closed")
    except AttributeError:
        raise RuntimeError("Serial connection is not configured")

def serial_read(ser: serial.Serial) -> str:
    _serial_check(ser)

    if ser.read_exception:
        raise ser.read_exception

    if ser._read_buff == b'':
        return(b'')

    data = ser._read_buff
    ser._read_buff = b''
    ser._read_flag.clear()
    return(data)


async def serial_read_async(ser: serial.Serial) -> str:
    _serial_check(ser)

    await ser._read_flag.wait()

    if ser.read_exception:
        raise ser.read_exception

    data = ser._read_buff
    ser._read_buff = b''
    ser._read_flag.clear()
    return(data)


def serial_init(ctrlsd: dict, name: str) -> serial.Serial:
    ctrlsd[name]['serial'] = serial.serial_for_url(
        ctrlsd[name]['port'],
        baudrate=ctrlsd[name]['buadrate'],
        write_timeout=0
    )                                                                                                   # write_timeout mast be set to 0 to make writes non-blocking

    ctrlsd[name]['serial'].controller_name = name
    ctrlsd[name]['serial']._read_buff = b''
    ctrlsd[name]['serial']._write_buff = b''
    ctrlsd[name]['serial']._read_flag = asyncio.Event()
    ctrlsd[name]['serial']._write_flag = asyncio.Event()
    ctrlsd[name]['serial'].read_exception = None
    ctrlsd[name]['serial'].write_exception = None

    return ctrlsd[name]['serial']


async def serial_loop(ctrlsd: dict, name: str):
    log = logging.getLogger("__main__")

    log.info(f"Entering '{name}' serial loop..")

    while True:
        while not ctrlsd[name]['serial'].is_open:
            try:
                serial_init(ctrlsd, name)
            except serial.serialutil.SerialException as e:
                log.critical(f"Failed to open serial connection to controller '{name}': {e}, retrying")
                await asyncio.sleep(1)
                pass
            else:
                log.info(f"Serial connection to controller '{name}' established")
                break

        asr_t = asyncio.create_task(_serial_async_writer(ctrlsd[name]['serial']))
        asw_t = asyncio.create_task(_serial_async_reader(ctrlsd[name]['serial']))
        try:
            await asyncio.gather(asr_t, asw_t)
        except serial.serialutil.SerialException as e:
            log.error(f"Serial connection Error '{name}': {e}")
            asr_t.cancel(msg=e)
            try:
                await asr_t
            except:
                pass
            asw_t.cancel(msg=e)
            try:
                await asw_t
            except:
                pass
            ctrlsd[name]['serial'].close()
            pass
        except asyncio.CancelledError:
            break

    """ Gracefull shutdown """
    log.info("Shutting down serial loop")
    asr_t.cancel()
    try:
        await asr_t
    except:
        pass
    asw_t.cancel()
    try:
        await asw_t
    except:
        pass

    ctrlsd[name]['serial'].close()


async def terminal(ctrlsd: dict, name: str):
    log = logging.getLogger("__main__")
    while True:
        while (ctrlsd[name]['serial'].is_open):
#        data = ''.join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(8))
#        log.debug(data)
#        await serial_write(ser, data + '\r\n')

#        data = serial_read(ser)
#        if len(data):
#            log.info(f"{data}")
#            serial_write(ser, data)
            try:
                data = await serial_read_async(ctrlsd[name]['serial'])
            except asyncio.CancelledError:
                log.info("Shutting down terminal loop")
                return None
            except serial.serialutil.SerialException:
                log.error("Reader is not available, waiting for serial connection to become ready")
                break
            else:
                log.info(f"{data.decode('ascii')}")
                await serial_write_async(ctrlsd[name]['serial'], data)

        await asyncio.sleep(1)


async def main_loop():
    controllers_d = { 'main': { 'port': '/tmp/pty0', 'buadrate': 115200, 'serial': None } }
    log = logging.getLogger("__main__")
    """Add signal handlers"""
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(getattr(signal, 'SIGINT'), functools.partial(handler_shutdown, 'SIGINT'))
    loop.add_signal_handler(getattr(signal, 'SIGTERM'), functools.partial(handler_shutdown, 'SIGTERM'))

    try:
        serial_init(controllers_d, 'main')
    except serial.serialutil.SerialException as e:
        log.critical(f"Failed to open serial connection to controller 'main': {e}")
        sys.exit(1)

    await asyncio.gather(serial_loop(controllers_d, 'main'), terminal(controllers_d, 'main'))


def main():
    """ Setup logging """
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt='%(asctime)s.%(msecs)03d asynctest: (%(levelname).1s) %(message)s', datefmt="%H:%M:%S"))
    log = logging.getLogger(__name__)
    log.handlers.clear()
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    log.info(f"Starting serial-sender test program v{_version_}..")

    asyncio.run(main_loop())

    log.info(f"Shutting down serial test program v{_version_}..")


if __name__ == "__main__":
    main()

"""Serial debug usage:
socat -d -d pty,rawer,b115200,link=/tmp/pty0 pty,rawer,b115200,link=/tmp/pty1
socat -d -d pty,raw,echo=1,b115200 pty,raw,echo=1,b115200
cu -s 115200 -l /tmp/pty1
TODO: try aioserial 1.3.1, pyserial_asyncio
"""

