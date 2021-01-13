#!/usr/bin/env python3.9
# vim: lw=-c\ scheduler.yaml

import sys
import argparse
import logging
import logging.handlers
import yaml
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

    peripherals = {}
    for entry in os.scandir(config['peripherals']):
        if entry.is_file() and (entry.name.endswith(".yaml") or entry.name.endswith(".yml")):
            with open(entry.path) as f:
                newpyaml = yaml.safe_load(f)
            for newpdev in newpyaml:
                if newpdev not in peripherals:
                    peripherals |= { newpdev: newpyaml[newpdev] }
                else:
                    print('Already exist: ', newpdev)

    print(json.dumps(peripherals, indent=2, sort_keys=True))

#    print(peripherals['light']['states']['on'])

#    for key in peripherals:
#        print(key, '::', peripherals[key])
#
#    while True:
#        time.sleep(10)
#        log.debug('10 sec')

#    log.critical('Failed')

    logging.shutdown()

if __name__ == "__main__":
    main()
