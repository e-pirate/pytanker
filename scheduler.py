#!/usr/bin/env python3.9
# vim: lw=-c\ scheduler.yaml

import sys
import argparse
import logging
import logging.handlers
import yaml
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

_version_ = '0.0.1'
_author_ = 'Artem Illarionov <e-pirate@mail.ru>'

def main():
    parser = argparse.ArgumentParser(add_help=True, description='Aquarium scheduler and queue manager daemon.')
    parser.add_argument('-c', nargs='?', required=True, metavar='file', help='Scheduler configuration file in YAML format', dest='config')
    args = parser.parse_args()

    """ Load configuration from YAML """
    try:
        with open(args.config) as f:
            config = yaml.safe_load(f)
    except OSError as e:
        sys.exit('scheduler: (C) Failed to load config: ' + str(e.strerror) + ': \'' + str(e.filename) + '\'')
    except yaml.YAMLError as e:
        sys.exit('scheduler: (C) Failed to parse config: ' + str(e))


    """ Setup logging """
    def setLogDestination(dst):
        if dst == 'console':
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(fmt='scheduler: (%(levelname).1s) %(message)s'))
        elif dst == 'syslog':
            handler = logging.handlers.SysLogHandler(facility=logging.handlers.SysLogHandler.LOG_DAEMON, address = '/dev/log')
            handler.setFormatter(logging.Formatter(fmt='scheduler[%(process)d]: (%(levelname).1s) %(message)s'))
        else:
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
        log.error('Failed to configure log: Destination is undefined. Failing over to syslog.')
    except ValueError:
        log.error('Failed to configure log: Unknown destination: \'' + config['log']['destination'] + '\'. Failing over to syslog.')
     
    try:
        log.setLevel(config['log']['level'].upper())
    except KeyError:
        log.error('Failed to configure log: Log level is undefined. Failing over to info.')
    except ValueError:
        log.error('Failed to configure log: Unknown level: \'' + config['log']['level'] + '\'. Failing over to info.')

    log.info('Starting scheduler v' + _version_ + '..')
    log.debug('Log level set to: ' + logging.getLevelName(log.level))

    class MyResolver(BaseResolver):
        pass

    MyResolver.add_implicit_resolver(
       u'tag:yaml.org,2002:bool',
       re.compile(u'''^(?:true|True|TRUE|false|False|FALSE)$''', re.X),
       list(u'tTfF'))

    class MyLoader(Reader, Scanner, Parser, Composer, SafeConstructor, MyResolver):
        def __init__(self, stream):
            Reader.__init__(self, stream)
            Scanner.__init__(self)
            Parser.__init__(self)
            Composer.__init__(self)
            SafeConstructor.__init__(self)
            MyResolver.__init__(self)

    devices = {}
    for entry in os.scandir(config['devices']):
        if entry.is_file() and (entry.name.endswith(".yaml") or entry.name.endswith(".yml")):
            with open(entry.path) as f:
                newdyaml = yaml.load(f, Loader=MyLoader)
            for newdev in newdyaml:
                if newdev not in devices: # TODO: should be moved to pre check procedure
                    devices = {**devices, newdev: newdyaml[newdev]}
                else:
                    log.error('Peripheral device: \'' + newdev + '\' already exist')

    if len(devices) == 0:
        log.crit('No peripheral devices found, unable to continue')
        sys.exit(1)
    log.info('Found ' + str(len(devices)) + ' peripheral device(s)')

    tasks = {}
    for entry in os.scandir(config['tasks']):
        if entry.is_file() and (entry.name.endswith(".yaml") or entry.name.endswith(".yml")):
            with open(entry.path) as f:
                newtyaml = yaml.load(f, Loader=MyLoader)
            for newtask in newtyaml:
                if newtask not in tasks: # TODO: should be moved to pre check procedure
                    tasks = {**tasks, newtask: newtyaml[newtask]}
                else:
                    log.error('Task: \'' + newtask + '\' already exist')

    if len(tasks) == 0:
        log.crit('No tasks found, unable to continue')
        sys.exit(1)
    log.info('Found ' + str(len(tasks)) + ' task(s)')

    statedb = {}
    for task in tasks:
        statedb[task] = {}
        for state in tasks[task]['states']:
            statedb[task][state['name']] = 'unknown'

    if len(statedb) == 0:
        log.crit('Failed to form state DB, unable to continue')
        sys.exit(1)
    log.info('Formed state DB for ' + str(len(statedb)) + ' tasks')


    log.info('Entering main event loop..')

#    print(json.dumps(statedb, indent=2, sort_keys=True))

#TODO: check if start is preore stop, duration < 1d + call check_cnd_time function and check for exceptions

    def checkcond_time(condition):
        now = datetime.now()

        def srtstp2tddt(timestr):
            if timestr.count(':') == 1:
                return(datetime.combine(now.date(), datetime.strptime(timestr, "%H:%M").time()))
            elif timestr.count(':') == 2:
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

            if (start + duration).day <= now.day:                                                       # check if task ends today
                if now > start + duration:                                                              # check if we already passed end time
                    return(False)
            else:                                                                                       # task will end tomorrow
                if start + duration - timedelta(days=1) < now < start:                                  # check if we already passed the remainig part of the end time
                    return(False)                                                                       # or did't reached start time yet
                else:                                                                                   # we are still withing the remainig part of the end time
                    return(True)                                                                        # return True now, as we are still withing the remaining part

        if now < start:                                                                                 # did not reached start time yet
            return(False)

        return(True)

    def checkcond_state(condition):
        return(True)

    def checkcond_power(condition):
        return(True)

    def checkcond(condition):
        if condition['type'] == 'time':
            return(checkcond_time(condition))
        if condition['type'] == 'state':
            return(checkcond_state(condition))
        if condition['type'] == 'power':
            return(checkcond_power(condition))

    # unknown -> inactive -> pending -> active
    while True:
        for task in tasks:
            for state in tasks[task]['states']:
                if state['name'] != 'default':
                    rc = 'active'
                    for condition in state['conditions']:
                        if not checkcond(condition):
                            rc = 'inactive'
                            break                                                                           # Stop checking conditions on first failure
                    if statedb[task][state['name']] != rc:
                        statedb[task][state['name']] = rc
#                        log.info('Chaging ' + task + ' state ' + statedb[task][state['name']] + ' -> ' + state['name'])
#        print(json.dumps(statedb, indent=2, sort_keys=True))
        time.sleep(1)

#    log.critical('Failed')

    logging.shutdown()

if __name__ == "__main__":
    main()
