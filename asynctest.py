#!/usr/bin/env python3.9

import sys
import logging
import time
import os
import asyncio
import logging
import logging.handlers
import random


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
        log.debug('Previosly pending aftercheck canceled')
    else:
        if True in results:
            log.debug('All pending task checks finished, starting aftercheck: ' + str(results))
            task_dispatcher(tasks)
        else:
            log.debug('All pending task checks finished, no state changed')


def task_dispatcher(tasks):
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
        for t in asyncio.all_tasks():                                                                   # Cancel pending aftercheck, caller will be skipped
            if t._coro.__name__ == 'task_aftercheck':
                t.cancel()
        asyncio.create_task(task_aftercheck(pending_tasks + spawned_tasks))

    dispatcher_lock = False


async def dispatcher_loop(tasks):
    log = logging.getLogger("__main__") 
    log.info('Entering dispatcher loop..')

    while True:
        try:
            task_dispatcher(tasks)
            await asyncio.sleep(int(time.time()) + 1 - time.time())                                     # Schedule check for the next round upcoming second
        except asyncio.CancelledError:
            log.info('Shutting down dispatcher loop..')
            break


#async def main_loop(tasks):
#    await asyncio.gather(tasks_loop(tasks), tasks_loop(tasks))


def main():
    """ Setup logging """
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt='%(asctime)s.%(msecs)03d asynctest: (%(levelname).1s) %(message)s', datefmt="%H:%M:%S"))
    log = logging.getLogger(__name__)
    log.handlers.clear()
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    log.info('Starting asyncio test program')

    try:
        asyncio.run(dispatcher_loop(tasks))
    except KeyboardInterrupt:
        log.info('Received keyboard interrupt')


if __name__ == "__main__":
    main()
