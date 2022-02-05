#!/usr/bin/env python3.10
# vim: lw=-c\ scheduler.yaml

import sys
import argparse
import logging
import logging.handlers
import yaml
import asyncio
from yaml.composer import Composer
from yaml.constructor import SafeConstructor
from yaml.parser import Parser
from yaml.reader import Reader
from yaml.resolver import BaseResolver
from yaml.scanner import Scanner
import re
import time
import os
import json
from datetime import datetime, timedelta


_version_ = "0.0.1"
_author_ = "Artem Illarionov <e-pirate@mail.ru>"


def checkcond_time(condition: dict) -> bool:
#TODO: check if start is preore stop, duration < 1d + call check_cnd_time function and check for exceptions
    now = datetime.now()

    def srtstp2tddt(timestr):
        match timestr.count(':'):
            case 1:
                return(datetime.combine(now.date(), datetime.strptime(timestr, "%H:%M").time()))
            case 2:
                return(datetime.combine(now.date(), datetime.strptime(timestr, "%H:%M:%S").time()))
        raise ValueError

    if 'stop' in condition and now > srtstp2tddt(condition['stop']):                                # stop time is set and we already passed it
        return(False)

    start = srtstp2tddt(condition['start'])

    if 'duration' in condition:
        duration = condition['duration'].lower()
        hours = minutes = seconds = 0
        if 'h' in duration:
            hours, duration = duration.split('h')
        if 'm' in duration:
            minutes, duration = duration.split('m')
        if 's' in duration:
            seconds, duration = duration.split('s')
        duration = timedelta(hours=int(hours), minutes=int(minutes), seconds=int(seconds))

        if (start + duration).day <= now.day:                                                       # check if job ends today
            if now > start + duration:                                                              # check if we already passed end time
                return(False)
        else:                                                                                       # job will end tomorrow
            if start + duration - timedelta(days=1) < now < start:                                  # check if we already passed the remainig part of the end time
                return(False)                                                                       # or did't reached start time yet
            else:                                                                                   # we are still withing the remainig part of the end time
                return(True)                                                                        # return True now, as we are still withing the remaining part

    if now < start:                                                                                 # did not reached start time yet
        return(False)

    return(True)


def checkcond_state(condition: str) -> bool:
    return(True)


def checkcond_power(condition: str) -> bool:
    return(True)


def checkcond(condition: str) -> bool:
    match condition['type']:
        case 'time':
            return(checkcond_time(condition))
        case 'state':
            return(checkcond_state(condition))
        case 'power':
            return(checkcond_power(condition))


#TODO: возвращять из каждой функции, проверяющей таск true, если статус изменился, проверять если в очереди незавершенные задачи на прверку тасков. Если текущий
# последний и хотябы один вернул истину, запустить еще один диспатчер проверки всех статусов, но без встроенного продолжателя
# unknown -> inactive -> scheduled -> pending -> active
async def task_loop(jobs: dict, statedb: dict):
    log = logging.getLogger("__main__")
    log.info("Entering job event loop..")

    while True:
        nextrun_uts = int(time.time()) + 1                                                              # Save round second for the next cycle to be run
        state_update = False

        for job in jobs:
            for state in jobs[job]['states']:
                if state['name'] == 'default':                                                          # Skip default state
                    continue
                status = 'active'
                for condition in state['conditions']:                                                   # Cycle through all conditions for the current state
                    if not checkcond(condition):                                                        # Check if current condition failed
                        status = 'inactive'
                        break                                                                           # Stop checking conditions on first failure
                if statedb[job][state['name']] != status:
                    if status == 'active':
                        if statedb[job][state['name']] in ['scheduled', 'pending']:
                            break
                        else:
                            status = 'scheduled'
                    log.debug(f"Chaging {job} state {state['name']!r}: {statedb[job][state['name']]} -> {status}")
                    statedb[job][state['name']] = status
                    state_update = True

            # Check if default state is present and should be activated for current job
            if 'default' in statedb[job]:
                default = True
                for name in statedb[job]:
                    if name != 'default' and statedb[job][name] in ['scheduled', 'pending', 'active']:
                        default = False
                        break
                if default:
                    if statedb[job]['default'] not in ['scheduled', 'pending' 'active']:
                        log.debug(f"Chaging {job} state \'default\': {statedb[job]['default']} -> scheduled")
                        statedb[job]['default'] = 'scheduled'
                else:
                    if statedb[job]['default'] in ['scheduled', 'pending' 'active']:
                        log.debug(f"Chaging {job} state \'default\': {statedb[job]['default']} -> inactive")
                        statedb[job]['default'] = 'inactive'
#        print(json.dumps(statedb, indent=2, sort_keys=True))

        if state_update:
            log.debug("State update is scheduled")

        if not state_update:
            await asyncio.sleep(nextrun_uts - time.time())                                              # Wait if no state updates scheduled or till upcoming second


async def state_loop():
    log = logging.getLogger("__main__")
    log.info("Entering state event loop..")
    while True:
        await asyncio.sleep(0.5)


async def main_loop(jobs: dict, statedb: dict):
    await asyncio.gather(task_loop(jobs, statedb), state_loop())


def main():
    parser = argparse.ArgumentParser(add_help=True, description="Aquarium scheduler and queue manager daemon.")
    parser.add_argument('-c', nargs='?', required=True, metavar='file', help="Scheduler configuration file in YAML format", dest='config')
#TODO: Реализовать опцию проверки конфигурации
    parser.add_argument('-t', nargs='?', metavar='test', help="Test devices and jobs according to specified configuration", dest='test')
    args = parser.parse_args()

    """ Load configuration from YAML """
    try:
        with open(args.config) as f:
            config = yaml.safe_load(f)
    except OSError as e:
        sys.exit(f"scheduler: (C) Failed to load config: {e.strerror} : {e.filename!r}")
    except yaml.YAMLError as e:
        sys.exit(f"scheduler: (C) Failed to parse config: {e}")

    """ Setup logging """
    def setLogDestination(dst):
        match dst:
            case 'console':
                handler = logging.StreamHandler()
                handler.setFormatter(logging.Formatter(fmt='%(asctime)s.%(msecs)03d scheduler: (%(levelname).1s) %(message)s', datefmt="%H:%M:%S"))
            case 'syslog':
                handler = logging.handlers.SysLogHandler(facility=logging.handlers.SysLogHandler.LOG_DAEMON, address = '/dev/log')
                handler.setFormatter(logging.Formatter(fmt='scheduler[%(process)d]: (%(levelname).1s) %(message)s'))
            case _:
                raise ValueError
        log.handlers.clear()
        log.addHandler(handler)

    # Configure default logger
    log = logging.getLogger(__name__)
    setLogDestination('syslog')
    log.setLevel(logging.INFO)

    try:
        setLogDestination(config['log']['destination'].lower())
    except KeyError:
        log.error("Failed to configure log: Destination is undefined. Failing over to syslog.")
    except ValueError:
        log.error(f"Failed to configure log: Unknown destination: {config['log']['destination']!r}. Failing over to syslog.")

    try:
        log.setLevel(config['log']['level'].upper())
    except KeyError:
        log.error("Failed to configure log: Log level is undefined. Failing over to info.")
    except ValueError:
        log.error(f"Failed to configure log: Unknown level: {config['log']['level']!r}. Failing over to info.")

    log.info(f"Starting scheduler v{_version_} ..")
    log.debug(f"Log level set to: {logging.getLevelName(log.level)}")

    """ Configure custom resolver to treat various true/false string combinations as booleans """
    class CustomResolver(BaseResolver):
        pass

    CustomResolver.add_implicit_resolver(
       u'tag:yaml.org,2002:bool',
       re.compile(u'''^(?:true|True|TRUE|false|False|FALSE)$''', re.X),
       list(u'tTfF'))

    class CustomLoader(Reader, Scanner, Parser, Composer, SafeConstructor, CustomResolver):
        def __init__(self, stream):
            Reader.__init__(self, stream)
            Scanner.__init__(self)
            Parser.__init__(self)
            Composer.__init__(self)
            SafeConstructor.__init__(self)
            CustomResolver.__init__(self)

    """ Load devices """
    devices = {}
    for entry in os.scandir(config['devices']):
        if entry.is_file() and (entry.name.endswith(".yaml") or entry.name.endswith(".yml")):
            with open(entry.path) as f:
                newdyaml = yaml.load(f, Loader=CustomLoader)
            for newdev in newdyaml:
                if newdev not in devices: # TODO: should be moved to pre check procedure
                    devices = {**devices, newdev: newdyaml[newdev]}
                else:
                    log.error(f"Peripheral device: {newdev!r} already exist")

    if not devices:
        log.critical("No peripheral devices found, unable to continue")
        sys.exit(1)
    log.info(f"Found {str(len(devices))} peripheral device(s)")

    """ Load jobs """
    jobs = {}
    for entry in os.scandir(config['jobs']):
        if entry.is_file() and (entry.name.endswith(".yaml") or entry.name.endswith(".yml")):
            with open(entry.path) as f:
                newjyaml = yaml.load(f, Loader=CustomLoader)
            for newjob in newjyaml:
                if newjob not in jobs: # TODO: should be moved to pre check procedure
                    jobs = {**jobs, newjob: newjyaml[newjob]}
                else:
                    log.error(f"Job: {newjob!r} already exist")

    if not jobs:
        log.critical("No jobs found, unable to continue")
        sys.exit(1)
    log.info(f"Found {str(len(jobs))} job(s)")

    """ Generate an empty state DB from all job states """
    statedb = {}
    for job in jobs:
        statedb[job] = {}
        for state in jobs[job]['states']:
            statedb[job][state['name']] = 'unknown'

    if not statedb:
        log.critical("Failed to generate state DB, unable to continue")
        sys.exit(1)
    log.info(f"Generated state DB for {str(len(statedb))} jobs")
#    print(json.dumps(statedb, indent=2, sort_keys=True))

    asyncio.run(main_loop(jobs, statedb))

    log.info(f"Shutting down scheduler v{_version_} ..")

    logging.shutdown()


if __name__ == "__main__":
    main()
