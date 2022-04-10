#!/usr/bin/env python3.10

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

jobs = { 'light': { 'duration': 1 }, 'co2': { 'duration': 1.5 }, 'dummy': { 'duration': 2 } }
statedb = { 'light': { 'isPending': False }, 'co2': { 'isPending': False }, 'dummy': { 'isPending': False } }
dispatcher_lock = False


async def task_check(job: str) -> bool:
    log = logging.getLogger("__main__")
    duration = random.randint(0, int(jobs[job]['duration'] * 1000)) / 1000
    log.debug(f"Checking task: {job} ({duration}s) started")
    try:
        await asyncio.sleep(duration)
    except asyncio.CancelledError:
        log.warning(f"Checking of task: {job} cancelled")
        statedb[job]['isPending'] = False
        return False
    else:
        log.debug(f"Checking of task: {job} finished")
        statedb[job]['isPending'] = False
        if random.randint(0, 10) < 5:
            return False
        else:
            return True


def handler_confupdate(signame: str):                                                                   # ps aux | egrep 'python.*asynctest\.py' | awk '{ print $2 }' | xargs kill -1
    log = logging.getLogger("__main__")
    log.info(f"Received {signame}: updating configuration..")

    asyncio.create_task(conf_update())

async def conf_update():
    global dispatcher_lock
    log = logging.getLogger("__main__")

    update_start = datetime.now()

    dispatcher_lock = True
    log.debug("Dispatcher lock is set")

    pending_tasks = []
    for t in asyncio.all_tasks():
        match t._coro.__name__:
            case 'task_check':
                pending_tasks.append(t)
            case 'tasks_aftercheck':
                t.cancel()
    if pending_tasks:
        try:
            await asyncio.shield(tasks_stopwait(pending_tasks, timeout=None))                           # Try to wait for tasks to finish during timeout seconds
        except asyncio.CancelledError:
            log.warning("Config update terminated, cancelling pending tasks")

    dispatcher_lock = False
    log.debug("Dispatcher lock is unset")

def handler_shutdown(signame: str, loop: asyncio.AbstractEventLoop):
    log = logging.getLogger("__main__")
    log.info(f"Received {signame}: exiting..")

    for t in asyncio.all_tasks():                                                                       # Cancel the dispatcher loop, pending aftercheck
        if t._coro.__name__ == 'sender_loop':                                                       # will be cancelled automatically
            t.cancel()


async def tasks_stopwait(pending_tasks: list, timeout: int = 1):
    log = logging.getLogger("__main__")
    log.info(f"Waiting {['', str(timeout) + 's '][isinstance(timeout, int)]}for {len(pending_tasks)} task(s) to finish")

    group_task = asyncio.gather(*pending_tasks)
    try:
        await asyncio.wait_for(group_task, timeout)
    except asyncio.TimeoutError:
        log.warning("Some tasks were cancelled due to timeout")
    except asyncio.CancelledError:
        try:                                                                                            # awaiting for the group task and catching CancelledError
            await group_task                                                                            # exception is needed to prevent
        except asyncio.CancelledError:                                                                  # '_GatheringFuture exception was never retrieved' error
            pass                                                                                        # in case of receiving multiple SIGINT/SIGTERM during shutdown
    else:
        log.info("All pending tasks finished")


async def tasks_aftercheck(pending_tasks: list):
    log = logging.getLogger("__main__")
    log.debug(f"Aftercheck got {len(pending_tasks)} task(s) to wait for")

    try:
        results = []                                                                                    # Gathered coroutings should be shielded to keep them from
        results = await asyncio.shield(asyncio.gather(*pending_tasks))                                  # being terminated recursively by the cancelled aftercheck
    except asyncio.CancelledError:
        log.debug("Pending aftercheck cancelled")
    else:
        if True in results:
            log.debug(f"All pending tasks finished, starting dispatcher: {results}")
            dispatcher(jobs)
        else:
            log.debug("All pending tasks finished, no state changed")


def dispatcher(jobs: dict):
    global dispatcher_lock
    log = logging.getLogger("__main__")

    if dispatcher_lock:
        log.debug("Dispatcher lock is set, skipping run")
        return

    dispatcher_lock = True
    log.debug("Dispatcher started")

    """ Get list of the task checks that are still pending """
    pending_tasks = []
    for t in asyncio.all_tasks():
        if t._coro.__name__ == 'task_check':
            pending_tasks.append(t)

    """ Spawn check for all tasks that are not currently been checked """
    spawned_tasks = []
    pending_jobs = []
    for job in jobs:
        if statedb[job]['isPending']:
            pending_jobs.append(job)
            continue
        statedb[job]['isPending'] = True
        new_task = asyncio.create_task(task_check(job))
        spawned_tasks.append(new_task)
    if pending_jobs:
        log.debug(f"Pending task(s) that were skipped: {pending_jobs}")
    log.debug(f"Dispatcher finished: {len(pending_tasks)} task(s) were pending, {len(spawned_tasks)} new task(s) spawned")

    """ Spawn trailing aftercheck if new task checks were schedulled """
    if spawned_tasks:
        for t in asyncio.all_tasks():                                                                   # Cancel pending aftercheck
            if t._coro.__name__ == 'tasks_aftercheck':
                t.cancel()
        asyncio.create_task(tasks_aftercheck(pending_tasks + spawned_tasks))                            # Spawn new aftercheck for previosly pending and new tasks

    dispatcher_lock = False


async def sender_loop(ser):
    log = logging.getLogger("__main__")
    log.info("Entering sender loop..")

    """ Add signal handlers """
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(getattr(signal, 'SIGINT'), functools.partial(handler_shutdown, 'SIGINT', loop))
    loop.add_signal_handler(getattr(signal, 'SIGTERM'), functools.partial(handler_shutdown, 'SIGTERM', loop))
    loop.add_signal_handler(getattr(signal, 'SIGHUP'), functools.partial(handler_confupdate, 'SIGHUP'))
    loop.add_signal_handler(getattr(signal, 'SIGQUIT'), functools.partial(handler_confupdate, 'SIGQUIT')) # For debug purposes only

#    try:
#        reader, writer = await serial_asyncio.open_serial_connection(url='/dev/pts/11', baudrate=9600, timeout=0.1)
#    except:
#        log.error(f"Failed to open serial device")
#        sys.exit(1)

    """ Main infinity sender loop """
    while True:
        try:
#            dispatcher(jobs)
            data = ''.join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(4)).join("\r\n")
            try:
#                writer.write(b"AT\r\n")
#                writer.write(data.encode("ascii"))
#                loop = asyncio.get_running_loop()
                future = await loop.run_in_executor(None, ser.write(data.encode("ascii")))
#                result = await asyncio.wait_for(future, timeout=1)
#                log.debug(result)
            except Exception as e:
                log.error(e)
            await asyncio.sleep(int(time.time()) + 1 - time.time())                                     # Schedule check for the next round upcoming second
        except asyncio.CancelledError:
            log.info("Shutting down sender loop")
            break

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

def main():
    """ Setup logging """
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt='%(asctime)s.%(msecs)03d asynctest: (%(levelname).1s) %(message)s', datefmt="%H:%M:%S"))
    log = logging.getLogger(__name__)
    log.handlers.clear()
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    log.info(f"Starting serial-sender test program v{_version_}..")

    try:
        ser = serial.serial_for_url('/dev/pts/11', baudrate=115200, timeout=0.1)
    except Exception as e:
        log.critical(f"Failed to open serial device: {e}")
        sys.exit(1)

    asyncio.run(sender_loop(ser))

    log.info(f"Shutting down serial-sender test program v{_version_}..")


if __name__ == "__main__":
    main()
