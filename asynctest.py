#!/usr/bin/env python3.9

import sys
import logging
import logging.handlers
import time
import os
import asyncio
import functools
import signal
import random

_version_ = '0.2.1'

tasks = [ 'light', 'co2', 'dummy' ]
statedb = { 'light': { 'isPending': False }, 'co2': { 'isPending': False }, 'dummy': { 'isPending': False } }
dispatcher_lock = False


async def task_check(task):
    log = logging.getLogger("__main__") 
    duration = random.randint(0, 2000) / 1000
    log.debug('Checking task: ' + task + ' (' + str(duration) + 's) started')
    try:
        await asyncio.sleep(duration)
    except asyncio.CancelledError:
        log.debug('Checking of task ' + task + ' cancelled')
        statedb[task]['isPending'] = False
        return False
    else:
        log.debug('Checking of task ' + task + ' finished')
        statedb[task]['isPending'] = False
        if random.randint(0, 10) < 5:
            return False
        else:
            return True


async def task_aftercheck(pending_tasks):
    log = logging.getLogger("__main__") 
    log.debug('Aftercheck got ' + str(len(pending_tasks)) + ' task(s) to await for')
    try:
        results = []                                                                                    # Gathered coroutings should be shielded to keep them from
        results = await asyncio.shield(asyncio.gather(*pending_tasks))                                  # being terminated recursively by the canceled aftercheck
    except asyncio.CancelledError:
        log.debug('Pending aftercheck canceled')
    else:
        if True in results:
            log.debug('All pending task checks finished, starting aftercheck: ' + str(results))
            dispatcher(tasks)
        else:
            log.debug('All pending task checks finished, no state changed')


async def task_stopwait(pending_tasks):
    log = logging.getLogger("__main__")
    log.debug('Waiting for ' + str(len(pending_tasks)) + ' task(s) to finish')
    try:
        await asyncio.shield(asyncio.gather(*pending_tasks))                                            # being terminated recursively by the canceled aftercheck
    except asyncio.CancelledError:
        log.debug('Active waiter canceled')
    else:
        log.debug('All waiting task(s) finished')


def dispatcher(tasks):
    global dispatcher_lock
    log = logging.getLogger("__main__") 

    if dispatcher_lock:
        log.debug('Dispatcher is already active, skipping run')
        return

    dispatcher_lock = True
    log.debug('Dispatcher started')

    """ Get list of the task checks that are still pending """
    pending_tasks = []
    for t in asyncio.all_tasks():
        if t._coro.__name__ == 'task_check':
            pending_tasks.append(t)

    """ Spawn check for all tasks that are not currently been checked """
    spawned_tasks = []
    pending_tasks_names = []
    for tn in tasks:
        if statedb[tn]['isPending']:
            pending_tasks_names.append(tn)
            continue
        statedb[tn]['isPending'] = True
        new_task = asyncio.create_task(task_check(tn))
        spawned_tasks.append(new_task)
    if pending_tasks_names:
        log.debug('Pending task(s) checks that were skipped: ' + str(pending_tasks_names))
    log.debug('Dispatcher finished: ' + str(len(pending_tasks)) + ' task check(s) were pending, ' + str(len(spawned_tasks)) + ' new task check(s) spawned')

    """ Spawn trailing aftercheck if new task checks were schedulled """
    if len(spawned_tasks) > 0:
        for t in asyncio.all_tasks():                                                                   # Cancel pending aftercheck
            if t._coro.__name__ == 'task_aftercheck':
                t.cancel()
        asyncio.create_task(task_aftercheck(pending_tasks + spawned_tasks))                             # Spawn new aftercheck for previosly pending and new tasks

    dispatcher_lock = False


def handler_shutdown(signame, loop):
    log = logging.getLogger("__main__")
    log.info("Got %s: exiting.." % signame)

    for t in asyncio.all_tasks():                                                                       # Terminate the dispatcher loop, pending aftercheck
        if t._coro.__name__ == 'dispatcher_loop':                                                       # will be cancelled automatically
            t.cancel()


def handler_confupdate():
    log = logging.getLogger("__main__")
    log.info("Got SIGHUP: updating configuration files..")


async def dispatcher_loop(tasks):
    log = logging.getLogger("__main__") 
    log.info('Entering dispatcher loop..')

    """ Add signal handlers """
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(getattr(signal, 'SIGINT'), functools.partial(handler_shutdown, 'SIGINT', loop))
    loop.add_signal_handler(getattr(signal, 'SIGTERM'), functools.partial(handler_shutdown, 'SIGTERM', loop))
    loop.add_signal_handler(getattr(signal, 'SIGHUP'), functools.partial(handler_confupdate))

    while True:
        try:
            dispatcher(tasks)
            await asyncio.sleep(int(time.time()) + 1 - time.time())                                     # Schedule check for the next round upcoming second
        except asyncio.CancelledError:
            log.info('Shutting down dispatcher loop')
            pending_tasks = []
            for t in asyncio.all_tasks():                                                               # Cancel pending aftercheck
                if t._coro.__name__ == 'task_check':
                    pending_tasks.append(t)
                elif t._coro.__name__ == 'task_aftercheck':
                    t.cancel()
            await asyncio.shield(task_stopwait(pending_tasks))

            break


def main():
    """ Setup logging """
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt='%(asctime)s.%(msecs)03d asynctest: (%(levelname).1s) %(message)s', datefmt="%H:%M:%S"))
    log = logging.getLogger(__name__)
    log.handlers.clear()
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    log.info('Starting asyncio test program v' + _version_ + '..')

    asyncio.run(dispatcher_loop(tasks))

    log.info('Shutting down scheduler v' + _version_ + '..')


if __name__ == "__main__":
    main()
