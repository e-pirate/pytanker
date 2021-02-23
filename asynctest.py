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
    log.debug('Checking task: ' + task + ' (' + str(duration) + 'ms)')
    await asyncio.sleep(duration)
    log.debug('Checking of task ' + task + ' finished')
    statedb[task]['isPending'] = False
    if random.randint(0, 10) < 5:
        return False
    else:
        return True

async def task_aftercheck(pending_tasks):
    log = logging.getLogger("__main__") 
    log.debug('Aftercheck got ' + str(len(pending_tasks)) + ' task(s) to await for')
    results = []
    results = await asyncio.shield(asyncio.gather(*pending_tasks))                                      # 'Gathered' coroutings should be 'shielded' to keep them from
                                                                                                        # being terminated recursively by the canceled aftercheck
    if True in results:
        log.debug('All pending task checks finished, starting aftercheck: ' + str(results))
        task_dispatcher(tasks)

def task_dispatcher(tasks) -> int:
    global dispatcher_lock
    log = logging.getLogger("__main__") 

    if dispatcher_lock:
        log.debug('Dispatcher is already active, skipping run')
        return 0

    dispatcher_lock = True
    log.debug('Dispatcher started') #: ' + str(statedb))

    """ Get list of the task checks that are still pending """
    pending_tasks = []
    for t in asyncio.all_tasks():
        if t._coro.__name__ == 'task_check':
            pending_tasks.append(t)

    spawned_tasks = []
    for t in tasks:
        if statedb[t]['isPending']:
            log.debug('Pending check for task ' + t + ' found, skipped')
            continue
        statedb[t]['isPending'] = True
        new_task = asyncio.create_task(task_check(t))
        spawned_tasks.append(new_task)
    log.debug('Dispatcher finished: ' + str(len(pending_tasks)) + ' task check(s) pending, ' + str(len(spawned_tasks)) + ' task check(s) spawned') #: ' + str(statedb))

    if len(spawned_tasks) > 0:                                                                          # Lunch new aftercheck only if any new checks were spawned during this cycle
        new_aftercheck = asyncio.create_task(task_aftercheck(pending_tasks + spawned_tasks))
        
        for t in asyncio.all_tasks():                                                                   # Cancel all pending afterchecks excluding new one
            if t._coro.__name__ == 'task_aftercheck' and t != new_aftercheck:
                t.cancel()
                log.debug('Pending aftercheck canceled')

    dispatcher_lock = False
    return len(spawned_tasks)

async def tasks_loop(tasks):
    log = logging.getLogger("__main__") 
    log.info('Entering task loop..')
    while True:
        task_dispatcher(tasks)
        await asyncio.sleep(int(time.time()) + 1 - time.time())                                         # Schedule check for the next round upcoming second

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

    asyncio.run(tasks_loop(tasks))

if __name__ == "__main__":
    main()