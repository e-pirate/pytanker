#!/usr/bin/env python3.11

import sys
import logging
import time
import os
import asyncio
import concurrent.futures
import functools
import signal
import random
from datetime import datetime, timedelta

_version_ = "0.3.4"

jobs = { 'light': { 'duration': 1 }, 'co2': { 'duration': 1.5 }, 'ferts': { 'duration': 2 }, 'pump': { 'duration': 4 } }
statedb = { 'light': { 'is_pending': False }, 'co2': { 'is_pending': False }, 'ferts': { 'is_pending': False }, 'pump': { 'is_pending': False } }


def async_func_wrapper(func: callable, *args, **kwargs):
    """Wrap awaitable functions so they can be offloaded to multiprocess executor
    Usage: await loop.run_in_executor(executor, async_func_wrapper, asyncfunc, args)
    """
    loop = asyncio.new_event_loop()
    return loop.run_until_complete(func(*args, **kwargs))


async def task_check(job: str, executor: concurrent.futures.ProcessPoolExecutor, queue: asyncio.Queue()) -> bool:
    log = logging.getLogger("__main__")
    loop = asyncio.get_event_loop()

    duration = random.randint(0, int(jobs[job]['duration'] * 500)) / 1000
    log.debug(f"Checking of job '{job}' ({duration}s) started")

    try:
        await asyncio.sleep(duration / 2)                                                               # Non-blocking function
        await loop.run_in_executor(executor, time.sleep, duration / 2)                                  # Offload blocking function to be executed in multiprocessing pool
    except asyncio.CancelledError:
        log.warning(f"Checking of job '{job}' cancelled")
        statedb[job]['is_pending'] = False
        return False
    else:
        if random.randint(0, 10) < 5:
            log.debug(f"Checking of job '{job}' completed: target state not updated")
            statedb[job]['is_pending'] = False
            return False
        else:
            log.debug(f"Checking of job '{job}' completed: targed state updated, adding to queue")
            await queue.put(job)
            return True


async def dummy_job(job: str) -> bool:
    log = logging.getLogger("__main__")

    duration = random.randint(0, int(jobs[job]['duration'] * 1000)) / 1000
    log.debug(f"Dummy execution of job '{job}' ({duration}s) started")
    try:
        await asyncio.sleep(duration)
    except asyncio.CancelledError:
        log.warning(f"Dummy execution of job '{job}' cancelled")
        return False
    else:
        if random.randint(0, 10) < 3:
            return False
        else:
            return True


async def async_shutdown():
    log = logging.getLogger("__main__")
    dispatcherloop_t = queueloop_t = None

    for _ in asyncio.all_tasks():
        match _._coro.__name__:
            case 'dispatcher_loop':
                dispatcherloop_t = _
            case 'queue_loop':
                queueloop_t = _
            case 'conf_update':
                _.cancel()

    if dispatcherloop_t:
        dispatcherloop_t.cancel()                                                                       # Cancel the dispatcher loop, pending tasks aftercheck will be cancelled automatically
        try:
            await dispatcherloop_t                                                                      # Try to wait for checks to complete during timeout
        except asyncio.CancelledError:
            log.warning("Dispatcher loop terminated abnormally")

    if queueloop_t:
        queueloop_t.cancel()
        try:
            await queueloop_t                                                                           # Try to wait for workers to complete theris tasks during timeout
        except asyncio.CancelledError:
            log.warning("Queue terminated abnormally")


def handler_shutdown(signame: str):
    log = logging.getLogger("__main__")

    log.info(f"Received {signame}: exiting..")
    asyncio.create_task(async_shutdown())


def handler_confupdate(signame: str, lock: asyncio.Lock()):
    log = logging.getLogger("__main__")

    log.info(f"Received {signame}: updating configuration..")
    asyncio.create_task(conf_update(lock))


async def conf_update(lock: asyncio.Lock()):
    log = logging.getLogger("__main__")

    async with lock:
        update_start = datetime.now()                                                                   # Save current time for future use

        log.debug("Dispatcher lock is set")

        #TODO: update as in dispatcher_loop
        """Collect all pending check tasks and cancel aftercheck if any"""
        pending_checks = []
        for _ in asyncio.all_tasks():
            match _._coro.__name__:
                case 'aftercheck':                                                                      # Cancel pending aftercheck to prevent new checks
                    _.cancel()                                                                          # from spawning during configuration update
                case 'task_check':
                    pending_checks.append(_)

        """Wait for all previously pending check tasks to finish"""
        if pending_checks:
            stopwait_t = asyncio.create_task(tasks_stopwait(pending_checks, timeout=3, msg='check'))
            try:
                await asyncio.shield(stopwait_t)
            except asyncio.CancelledError:
                log.warning("Configuration update cancelled")
                stopwait_t.cancel()
                return

        log.info("Performin configuration update")
        # FIXME: time sweep complete

    log.debug("Dispatcher lock reseted")


async def tasks_stopwait(pending_tasks: list, timeout: int = 1, msg: str = 'task'):
    """Wait for all pending tasks to finish and exit"""
    log = logging.getLogger("__main__")
    log.info(f"Waiting {['', str(timeout) + 's '][isinstance(timeout, int)]}for {len(pending_tasks)} {msg}(s) to finish")

    result = None
    group_t = asyncio.gather(*pending_tasks)
    try:                                                                                                # shielding prevents pending tasks from being cancelled
        result = await asyncio.shield(asyncio.wait_for(group_t, timeout))                               # if stopwait was cancelled from outside
    except asyncio.TimeoutError:
        log.warning(f"Some {msg}s were cancelled due to timeout: {result}")
    except asyncio.CancelledError:
        try:                                                                                            # awaiting for the group task and catching CancelledError
            result = await group_t                                                                      # exception is needed to prevent
        except asyncio.CancelledError:                                                                  # '_GatheringFuture exception was never retrieved' error
            pass                                                                                        # in case of receiving multiple SIGINT/SIGTERM during shutdown
    else:
        log.info(f"All pending {msg}s finished: {result}")

    return result


async def aftercheck(pending_checks: list, jobs: dict, executor: concurrent.futures.ProcessPoolExecutor, queue: asyncio.Queue()) -> bool:
    log = logging.getLogger("__main__")

    """First wait for all pending check tasks to complete. If aftercheck happen to start dispatcher
    before so, unfinished checks may complete with state update after dispatcher run and this updates will
    not be taken into account until next dispatcher cycle.
    """
    log.debug(f"Aftercheck got {len(pending_checks)} checks(s) to wait for")
    try:                                                                                                # Gathered coroutings should be shielded to keep them from
        await asyncio.shield(asyncio.gather(*pending_checks))                                           # being terminated recursively by the cancelled aftercheck
    except asyncio.CancelledError:
        log.debug("Pending aftercheck cancelled")
        return False

    """Second insure that queue is empty and all workers completed processing jobs by calling queue.join()
    that will unblock only after worker processing the last item from the queue is done and update the flag.
    """
    busy_workers = 0
    for _ in queue.brigade:
        if not _.idling:
            busy_workers += 1
    if queue.qsize() > 0 or busy_workers > 0:
        log.debug(f"Aftercheck waiting for {queue.qsize()} item(s) in queue and {busy_workers} worker(s) to complete, flag is {['clear', 'set'][queue.aftercheck]}")
        try:
            await asyncio.shield(queue.join())
        except asyncio.CancelledError:
            log.debug("Pending aftercheck cancelled")
            return False

    """If aftercheck flag is set by any of the workers indicating there were an actual state change we
    should run dispatcher to check for any upcoming updates.
    """
    if queue.aftercheck:
        log.debug("All pending jobs completed, state changed, launching dispatcher")
        dispatcher(jobs, executor, queue)
        return False
    else:
        log.debug("All pending jobs completed, state not changed")
        return True


def dispatcher(jobs: dict, executor: concurrent.futures.ProcessPoolExecutor, queue: asyncio.Queue()):
    log = logging.getLogger("__main__")

    log.debug("Dispatcher started")

    if queue.aftercheck:
        queue.aftercheck = False                                                                        # Clear aftercheck flag because we are checking now
        log.debug("Aftercheck flag cleared")

    # TODO: move to asyncio TaskGroup
    """Get list of the task checks that are still pending"""
    pending_checks = []
    for _ in asyncio.all_tasks():
        if _._coro.__name__ == 'task_check':
            pending_checks.append(_)

    """Spawn check for all tasks that are not currently been checked"""
    spawned_checks = []
    pending_jobs = []
    for job in jobs:
        if statedb[job]['is_pending']:
            pending_jobs.append(job)
            continue
        statedb[job]['is_pending'] = True
        new_t = asyncio.create_task(task_check(job, executor, queue))
        spawned_checks.append(new_t)
    if pending_jobs:
        log.debug(f"Pending job(s) that were skipped: {pending_jobs}")
    log.debug(f"Dispatcher finished: {len(pending_checks)} check(s) were pending, {len(pending_jobs) - len(pending_checks)} job(s) were processing, {len(spawned_checks)} new check(s) spawned")

    """Spawn trailing afterchecks if new check tasks were schedulled"""
    if spawned_checks:
        for _ in asyncio.all_tasks():
            if _._coro.__name__ == 'aftercheck':
                _.cancel()
        asyncio.create_task(aftercheck(pending_checks + spawned_checks, jobs, executor, queue))         # Spawn new aftercheck for previosly pending and new tasks


async def dispatcher_loop(jobs: dict, executor: concurrent.futures.ProcessPoolExecutor, queue: asyncio.Queue(), lock: asyncio.Lock()):
    log = logging.getLogger("__main__")

    """Main infinity dispatcher loop"""
    log.info("Entering dispatcher loop..")
    while True:
        try:
            if not lock.locked():
                async with lock:
                    dispatcher(jobs, executor, queue)
            else:
                log.debug("Dispatcher lock is set, skipping run")

            await asyncio.sleep(int(time.time()) + 1 - time.time())                                     # Schedule check for the next round upcoming second
        except asyncio.CancelledError:
            break

    """Gracefull shutdown"""
    log.info("Shutting down dispatcher loop")
    pending_checks = []
    for _ in asyncio.all_tasks():
        match _._coro.__name__:
            case 'aftercheck':                                                                          # Cancel pending aftercheck ASAP
                _.cancel()
            case 'task_check':                                                                          # Collect active check tasks
                pending_checks.append(_)
    if pending_checks:
        try:
            await asyncio.shield(tasks_stopwait(pending_checks, timeout=1, msg='check'))                # Try to wait for tasks to finish during timeout seconds
        except asyncio.CancelledError:
            log.warning("Graceful shutdown terminated, cancelling pending checks")
            for _ in pending_checks:
                if not _.done():                                                                        # Cancel leftover checks to prevent zombies
                    _.cancel()
                    try:
                        await _                                                                         # Ensure check actually quited
                    except:
                        pass

    log.info("Dispatcher loop is stopped")


async def worker(queue: asyncio.Queue(), queueloop_t: asyncio.Task, num: int):
    log = logging.getLogger("__main__")

    while True:
        asyncio.current_task().idling = True

        if queueloop_t.stopping and queue.empty():
            log.debug(f"Queue is empty and stopping, worker[{num}] quited ")
            return None

        try:
            job = await queue.get()
        except asyncio.CancelledError:
            log.debug(f"Idling worker[{num}] was cancelled")
            return None

        asyncio.current_task().idling = False
        log.debug(f"Worker[{num}] started processing job '{job}'")

        for attempt in range(1, 4):
            if attempt > 1:
                log.warning(f"Worker[{num}] failed to process job '{job}', retrying {attempt}/3")
            job_t = asyncio.create_task(dummy_job(job))
            try:
                result = await asyncio.shield(job_t)
            except asyncio.CancelledError:
                log.warning(f"Active worker[{num}] was cancelled, returning job '{job}' to queue")
                job_t.cancel()                                                                          # Propagate cancellation further to the child task
                try:
                    await job_t                                                                         # Ensure the child task is actually cancelled and quited
                except:
                    pass
                try:
                    await queue.put(job)                                                                # Return job to the end of the queue
                except:
                    statedb[job]['is_pending'] = False                                                  # If not, just reset state so dispatcher can take it later
                    pass
                queue.task_done()
                return None
            else:
                if result:
                    log.debug(f"Worker[{num}] successfully processed job '{job}'")
                    statedb[job]['is_pending'] = False
                    queue.aftercheck = True                                                             # Set flag to indicate state change for upcoming aftercheck
                    break

        if not result:
            log.error(f"Worker[{num}] was unable to process job '{job}', returning to queue")
            try:
                await queue.put(job)                                                                    # Return job to the end of the queue giving workers chance to process more recent jobs
            except:
                statedb[job]['is_pending'] = False                                                      # If not, just reset state so dispatcher can take it later
                pass

        queue.task_done()


async def queue_loop(queue: asyncio.Queue(), workers: int = 2):
    log = logging.getLogger("__main__")
    log.info(f"Starting queue with {workers} worker(s)..")

    queue.aftercheck = False
    asyncio.current_task().stopping = False

    queue.brigade = []
    for _ in range(workers):
        queue.brigade.append(asyncio.create_task(worker(queue=queue, queueloop_t=asyncio.current_task(), num=_)))

    try:
        await asyncio.shield(asyncio.gather(*queue.brigade))
    except asyncio.CancelledError:
        pass

    """Gracefull shutdown"""
    log.info("Shutting down queue")

    asyncio.current_task().stopping = True

    for _ in reversed(range(len(queue.brigade))):
        if queue.brigade[_].idling:                                                                     # First cancell all idling workers
            queue.brigade[_].cancel()
            try:
                await queue.brigade[_]                                                                  # Ensure worker actually quited
            except:
                pass
            else:
                del queue.brigade[_]                                                                    # Remove quited worker from brigade

    if queue.brigade:
        log.debug(f"Queue has {queue.qsize()} item(s) and {len(queue.brigade)} worker(s) processing jobs")
        try:
            await asyncio.shield(tasks_stopwait(queue.brigade, timeout=2, msg='worker'))                # Try to wait for workers to complete theris tasks during timeout seconds
        except asyncio.CancelledError:
            log.warning("Graceful shutdown terminated, cancelling all workers")
            for _ in queue.brigade:
                _.cancel()                                                                              # Cancel leftover workers to prevent zombies
                try:
                    await _                                                                             # Ensure worker actually quited
                except:
                    pass

    log.info(f"Queue is stopped having {queue.qsize()} unprocessed item(s)")


def worker_initializer():
    """Let main process take care of signals by ignoring handled signals in worker process"""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGQUIT, signal.SIG_IGN)


async def main_loop(jobs: dict):
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=[None, 4][os.cpu_count() >= 4], initializer=worker_initializer)   # Initilaze multiprocessing executor pool limited to 4 processes maximum
    dispatcher_lock = asyncio.Lock()
    queue = asyncio.Queue()

    """Add signal handlers"""
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(getattr(signal, 'SIGINT'), functools.partial(handler_shutdown, 'SIGINT'))
    loop.add_signal_handler(getattr(signal, 'SIGTERM'), functools.partial(handler_shutdown, 'SIGTERM'))
    loop.add_signal_handler(getattr(signal, 'SIGHUP'), functools.partial(handler_confupdate, 'SIGHUP', dispatcher_lock))   # ps aux | egrep 'python.*asynctest\.py' | awk '{ print $2 }' | xargs kill -1
    loop.add_signal_handler(getattr(signal, 'SIGQUIT'), functools.partial(handler_confupdate, 'SIGQUIT', dispatcher_lock)) # For debug purposes only (Ctrl-\)

    await asyncio.gather(queue_loop(queue=queue), dispatcher_loop(jobs=jobs, executor=executor, queue=queue, lock=dispatcher_lock))

    executor.shutdown()


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


"""Docs:
https://docs.python.org/3.11/library/asyncio.html
https://docs.python.org/3/library/concurrent.futures.html
"""
