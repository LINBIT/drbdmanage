#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2016 LINBIT HA-Solutions GmbH
    Author: Roland Kammerer <roland.kammerer@linbit.com>

    For further information see the COPYING file.
"""
from datetime import datetime
import tempfile


class DbusTracer(object):
    def __init__(self):
        self.running = False
        self.log = []
        self.entries = 0
        self.last_time = None
        self.measure_time = False
        self.boilerplate = """#!/usr/bin/env python2

import dbus
import sys
import time
import os
from drbdmanage.consts import (DBUS_DRBDMANAGED, DBUS_SERVICE)

try:
    _dbus = dbus.SystemBus()
    _server = _dbus.get_object(DBUS_DRBDMANAGED, DBUS_SERVICE)
except dbus.exceptions.DBusException as exc:
    sys.stderr.write('DBUS connection failed')
    sys.exit(1)

enable_sleep = os.getenv('DMDT_SLEEP', False)
enable_stepping = os.getenv('DMDT_STEP', False)

def dt_sleep(t):
    if not enable_sleep:
        return
    else:
        print 'Sleeping for', t, 'seconds'
        time.sleep(t)

def dt_stepping():
    if not enable_stepping:
        return
    else:
        raw_input('Press <enter> to continue')
"""

    def start(self, maxlog=1000):
        if self.running:
            return False

        self.maxlog = maxlog
        self.log = []
        self.entries = 0
        self.last_time = None
        self.running = True

        return True

    def record(self, member, args):
        if not self.running:
            return

        if self.entries < self.maxlog:
            self.entries += 1
            sleep = '0'
            now = datetime.now()
            if self.last_time:
                sleep = now - self.last_time
                try:
                    sleep = sleep.total_seconds()  # returns float
                except:  # who cares about py 2.6...
                    sleep = float('%s.%s' % (sleep.seconds, sleep.microseconds))
            self.last_time = now

            self.log.append((sleep, member, args))
        else:  # wrap
            self.log = []
            self.entries = 0

    def stop(self):
        if not self.running:
            return False, ''

        failed = False
        self.running = False
        try:
            tmpf = tempfile.mkstemp(prefix='dmtrace_', suffix='.py')[1]
            fp = open(tmpf, 'w')
            fp.write(self.boilerplate)

            for line in self.log:
                sleep, member, args = line
                args = '%r' % (args)
                args = args[1:-1]
                if sleep != '0':
                    fp.write('dt_sleep(%f)\n' % (sleep))
                cmd = '_server.%s(%s)' % (member, args)
                fp.write('print\n')
                fp.write('print "Executing: %s"\n' % (cmd))
                fp.write('%s\n' % (cmd))
                fp.write('dt_stepping()\n')
        except:
            failed = True

        # gc the log
        self.log = []

        if failed:
            return False, ''
        else:
            return True, tmpf
