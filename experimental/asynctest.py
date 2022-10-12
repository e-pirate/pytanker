#!/usr/bin/env python3.10

import sys
import logging
import logging.handlers
import time
import os
import asyncio
import functools
import signal
import random
from datetime import datetime, timedelta

_version_ = "0.3.2"

jobs = { 'light': { 'duration': 1 }, 'co2': { 'duration': 1.5 }, 'micro': { 'duration': 2 }, 'macro': { 'duration': 4 } }
statedb = { 'light': { 'isPending': False }, 'co2': { 'isPending': False }, 'micro': { 'isPending': False }, 'macro': { 'isPending': False } }


async def task_check(job: str, queue: asyncio.Queue()) -> bool:
    log = logging.getLogger("__main__")

    duration = random.randint(0, int(jobs[job]['duration'] * 500)) / 1000
    log.debug(f"Checking: {job} ({duration}s) started")
    try:
        await asyncio.sleep(duration)
    except asyncio.CancelledError:
        log.warning(f"Checking of job: {job} cancelled")
        statedb[job]['isPending'] = False
        return False
    else:
        if random.randint(0, 10) < 5:
            log.debug(f"Checking of job: {job} finished: state not changed")
            statedb[job]['isPending'] = False
            return False
        else:
            log.debug(f"Checking of job: {job} finished: adding to queue")
            await queue.put(job)
            return True


async def dummy_task(job: str) -> bool:
    log = logging.getLogger("__main__")

    duration = random.randint(0, int(jobs[job]['duration'] * 1000)) / 1000
    log.debug(f"Dummy executing: {job} ({duration}s) started")
    try:
        await asyncio.sleep(duration)
    except asyncio.CancelledError:
        log.warning(f"Dummy executing of job: {job} cancelled")
        return False
    else:
        if random.randint(0, 10) < 2:
            log.debug(f"Dummy executing of job: {job} finished: state not changed")
            return False
        else:
            log.debug(f"Dummy executing of job: {job} finished: state changed")
            return True


def handler_shutdown(signame: str, loop: asyncio.AbstractEventLoop):
    log = logging.getLogger("__main__")
    log.info(f"Received {signame}: exiting..")

    for _ in asyncio.all_tasks():
        if _._coro.__name__ in ['dispatcher_loop', 'queue_loop', 'conf_update']:                        # Cancel the dispatcher loop, pending aftercheck
            _.cancel()                                                                                  # will be cancelled automatically


def handler_confupdate(signame: str, lock: asyncio.Lock()):                                             # ps aux | egrep 'python.*asynctest\.py' | awk '{ print $2 }' | xargs kill -1
    log = logging.getLogger("__main__")
    log.info(f"Received {signame}: updating configuration..")

    asyncio.create_task(conf_update(lock))


async def conf_update(lock: asyncio.Lock()):
    log = logging.getLogger("__main__")

    async with lock:
        update_start = datetime.now()                                                                   # Save current time for future use

        log.debug("Dispatcher lock is set")

        """Collect all pending check tasks and cancel aftercheck if any"""
        pending_tasks = []
        for _ in asyncio.all_tasks():
            match _._coro.__name__:
                case 'tasks_aftercheck'|'queue_aftercheck':                                             # Cancel pending aftercheck to prevent new checks
                    _.cancel()                                                                          # from spawning during configuration update
                case 'task_check':
                    pending_tasks.append(_)

        """Wait for all previously pending check tasks to finish"""
        if pending_tasks:
            sw_task = asyncio.create_task(tasks_stopwait(pending_tasks, timeout=3))
            try:
                await asyncio.shield(sw_task)
            except asyncio.CancelledError:
                log.warning("Configuration update cancelled")
                sw_task.cancel()
                return

        log.info("Performin configuration update")

        log.debug("Dispatcher lock is unset")


async def tasks_stopwait(pending_tasks: list, timeout: int = 1):
    """Wait for all pending tasks to finish and exit"""
    log = logging.getLogger("__main__")
    log.info(f"Waiting {['', str(timeout) + 's '][isinstance(timeout, int)]}for {len(pending_tasks)} task(s) to finish")

    result = None
    group_task = asyncio.gather(*pending_tasks)
    try:                                                                                                # shielding prevents pending tasks from being cancelled
        result = await asyncio.shield(asyncio.wait_for(group_task, timeout))                            # if stopwait was cancelled from outside
    except asyncio.TimeoutError:
        log.warning(f"Some tasks were cancelled due to timeout: {result}")
    except asyncio.CancelledError:
        try:                                                                                            # awaiting for the group task and catching CancelledError
            result = await group_task                                                                   # exception is needed to prevent
        except asyncio.CancelledError:                                                                  # '_GatheringFuture exception was never retrieved' error
            pass                                                                                        # in case of receiving multiple SIGINT/SIGTERM during shutdown
    else:
        log.info(f"All pending tasks finished: {result}")

    return result


async def queue_aftercheck(queue: asyncio.Queue()):
    log = logging.getLogger("__main__")

    try:
        await queue.join()
    except asyncio.CancelledError:
        log.debug("Pending queue aftercheck cancelled")
    else:
        log.debug("Queue is empty")


async def tasks_aftercheck(pending_tasks: list, queue: asyncio.Queue()):
    """Wait for all pending checks to finish and start queue aftercheck if there were any state change"""
    log = logging.getLogger("__main__")
    log.debug(f"Aftercheck got {len(pending_tasks)} task(s) to wait for")

    results = []
    try:                                                                                                # Gathered coroutings should be shielded to keep them from
        results = await asyncio.shield(asyncio.gather(*pending_tasks))                                  # being terminated recursively by the cancelled aftercheck
    except asyncio.CancelledError:
        log.debug("Pending aftercheck cancelled")
    else:
        if True in results:
            log.debug(f"All pending checks finished, spawning queue aftercheck: {results}")
            asyncio.create_task(queue_aftercheck(queue))
        else:
            log.debug("All pending checks finished, no state changed")


def dispatcher(jobs: dict, queue: asyncio.Queue()):
    log = logging.getLogger("__main__")

    log.debug("Dispatcher started")

    """Get list of the task checks that are still pending"""
    pending_tasks = []
    for _ in asyncio.all_tasks():
        if _._coro.__name__ == 'task_check':
            pending_tasks.append(_)

    """Spawn check for all tasks that are not currently been checked"""
    spawned_tasks = []
    pending_jobs = []
    for job in jobs:
        if statedb[job]['isPending']:
            pending_jobs.append(job)
            continue
        statedb[job]['isPending'] = True
        new_task = asyncio.create_task(task_check(job, queue))
        spawned_tasks.append(new_task)
    if pending_jobs:
        log.debug(f"Pending check(s) that were skipped: {pending_jobs}")
    log.debug(f"Dispatcher finished: {len(pending_tasks)} check(s) were pending, {len(spawned_tasks)} new check(s) spawned")

    """Spawn trailing afterchecks if new task checks were schedulled"""
    if spawned_tasks:
        for _ in asyncio.all_tasks():
            if _._coro.__name__ in ['tasks_aftercheck', 'queue_aftercheck']:
                _.cancel()
        asyncio.create_task(tasks_aftercheck(pending_tasks + spawned_tasks, queue))                     # Spawn new afterchecks for previosly pending and new tasks

async def dispatcher_loop(jobs: dict, lock: asyncio.Lock(), queue: asyncio.Queue()):
    log = logging.getLogger("__main__")
    log.info("Entering dispatcher loop..")

    """Main infinity dispatcher loop"""
    while True:
        try:
            if not lock.locked():
                async with lock:
                    dispatcher(jobs, queue)
            else:
                log.debug("Dispatcher lock is set, skipping run")

            await asyncio.sleep(int(time.time()) + 1 - time.time())                                     # Schedule check for the next round upcoming second
        except asyncio.CancelledError:
            break

    """Gracefull shutdown"""
    log.info("Shutting down dispatcher loop")
    pending_tasks = []
    for _ in asyncio.all_tasks():
        match _._coro.__name__:
            case 'tasks_aftercheck':                                                                    # Cancel pending aftercheck ASAP
                _.cancel()
            case 'task_check':                                                                          # Collect active check tasks
                pending_tasks.append(_)
    if pending_tasks:
        try:
            await asyncio.shield(tasks_stopwait(pending_tasks, timeout=1))                              # Try to wait for tasks to finish during timeout seconds
        except asyncio.CancelledError:
            log.warning("Graceful shutdown terminated, cancelling pending checks")
    log.info("Dispatcher loop is stopped")


async def worker(queue: asyncio.Queue(), ql_task: asyncio.Task, num: int):
    log = logging.getLogger("__main__")

    while True:
        if ql_task.stopping and queue.empty():
            log.debug(f"Queue loop was cancelled, quiting worker-{num}")
            break

        log.debug(f"Entering worker-{num} loop")
        asyncio.current_task().idling = True
        try:
            job = await queue.get()
        except asyncio.CancelledError:
            log.debug(f"Idling worker-{num} cancelled")
            break

        asyncio.current_task().idling = False
        log.debug(f"Worker-{num} started rocessing job: {job}")
        try:
            result = await asyncio.shield(dummy_task(job))
        except asyncio.CancelledError:
            log.warning(f"Busy worker-{num} was cancelled")
            statedb[job]['isPending'] = False
            queue.task_done()
            break
        else:
            log.debug(f"Worker-{num} completed processing job: {job}")
            statedb[job]['isPending'] = False
            queue.task_done()

    asyncio.current_task().idling = True


async def queue_loop(queue: asyncio.Queue(), workers: int = 2):
    log = logging.getLogger("__main__")
    log.info(f"Starting queue with {workers} worker(s) spawned..")

    asyncio.current_task().stopping = False

    brigade = []
    for _ in range(workers):
        brigade.append(asyncio.create_task(worker(queue=queue, ql_task=asyncio.current_task(), num=_)))

    try:
        await asyncio.shield(asyncio.gather(*brigade))
    except asyncio.CancelledError:
        """Gracefull shutdown"""
        log.info(f"Shutting down queue loop")
        dpl = None
        for _ in asyncio.all_tasks():
            if _._coro.__name__ in ['queue_aftercheck']:
                _.cancel()
        #TODO: wait for all checks to complete
            if _._coro.__name__ == 'dispatcher_loop':
                dpl = _
        if dpl:
            log.debug("Waiting for dispatcher loop to finish")
            try:
                await asyncio.shield(asyncio.wait_for(dpl, timeout=1))                                  # Try to wait for workers to complete theris tasks during timeout seconds
            except asyncio.CancelledError:
                log.warning("DPL terminated")
            else:
                log.debug("Dispatcher loop finished, stopping queue")
        asyncio.current_task().stopping = True
        for _ in reversed(range(len(brigade))):
            if brigade[_].idling:
                brigade[_].cancel()
                del brigade[_]
        if brigade:
            log.debug(f"{len(brigade)} worker(s) still processing jobs")
            try:
                await asyncio.shield(tasks_stopwait(brigade, timeout=2))                                # Try to wait for workers to complete theris tasks during timeout seconds
            except asyncio.CancelledError:
                log.warning("Graceful shutdown terminated, cancelling all workers")
    log.info("Queue is stopped")


async def main_loop(jobs: dict):
    dispatcher_lock = asyncio.Lock()
    queue = asyncio.Queue()

    """Add signal handlers"""
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(getattr(signal, 'SIGINT'), functools.partial(handler_shutdown, 'SIGINT', loop))
    loop.add_signal_handler(getattr(signal, 'SIGTERM'), functools.partial(handler_shutdown, 'SIGTERM', loop))
    loop.add_signal_handler(getattr(signal, 'SIGHUP'), functools.partial(handler_confupdate, 'SIGHUP', dispatcher_lock))
    loop.add_signal_handler(getattr(signal, 'SIGQUIT'), functools.partial(handler_confupdate, 'SIGQUIT', dispatcher_lock)) # For debug purposes only (Ctrl-\)

    await asyncio.gather(queue_loop(queue=queue), dispatcher_loop(jobs, lock=dispatcher_lock, queue=queue))


def main():
    """Setup logging"""
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt='%(asctime)s.%(msecs)03d asynctest: (%(levelname).1s) %(message)s', datefmt="%H:%M:%S"))
    log = logging.getLogger(__name__)
    log.handlers.clear()
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    log.info(f"Starting asyncio test program v{_version_}..")

    asyncio.run(main_loop(jobs=jobs))

    log.info(f"Shutting down asyncio test program v{_version_}..")


if __name__ == "__main__":
    main()
