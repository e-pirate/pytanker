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
from yaml.resolver import BaseResolver, Resolver as DefaultResolver
from yaml.scanner import Scanner
import re
import time
import os
import json

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
                if newdev not in devices:
                    devices = {**devices, newdev: newdyaml[newdev]}
                else:
                    log.error('Peripheral device: \'' + newdev + '\' already exist.')

    tasks = {}
    for entry in os.scandir(config['tasks']):
        if entry.is_file() and (entry.name.endswith(".yaml") or entry.name.endswith(".yml")):
            with open(entry.path) as f:
                newtyaml = yaml.load(f, Loader=MyLoader)
            for newtask in newtyaml:
                if newtask not in tasks:
                    tasks = {**tasks, newtask: newtyaml[newtask]}
                else:
                    log.error('Task: \'' + newtask + '\' already exist.')

#    print(json.dumps(devices, indent=2, sort_keys=True))
#    print(json.dumps(tasks, indent=2, sort_keys=True))
#    print(tasks)

    print(devices['light']['states']['on'])

#    for key in devices:
#        print(key, '::', devices[key])
#
#    while True:
#        time.sleep(10)
#        log.debug('10 sec')

#    log.critical('Failed')

    logging.shutdown()

if __name__ == "__main__":
    main()
