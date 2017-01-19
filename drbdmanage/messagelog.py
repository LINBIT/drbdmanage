#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013 - 2017  LINBIT HA-Solutions GmbH
                               Author: R. Altnoeder, Roland Kammerer

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import datetime
from collections import deque

class MessageLog(object):
    """
    Circular buffer message log
    """

    # Log levels
    INFO  = 0
    WARN  = 1
    ALERT = 2

    MIN_ENTRIES = 1
    MAX_ENTRIES = 10000

    def __init__(self, entries):
        """
        Initializes a new MessageLog with the specified capacity for log entries
        """
        # Declare instance variables
        self._log_entries = deque(maxlen=MessageLog.MAX_ENTRIES)
        self._capacity = None
        # Initialize the ring buffer
        self.resize(entries)

    def resize(self, entries):
        """
        Resets the MessageLog's capacity
        """
        if entries < MessageLog.MIN_ENTRIES or entries > MessageLog.MAX_ENTRIES:
            raise ValueError
        self._log_entries = deque(self._log_entries, maxlen=entries)
        self._capacity = entries

    def has_entries(self):
        """
        Returns True if there is at least one log entry
        """
        return len(self._log_entries)

    def get_capacity(self):
        """
        Returns the MessageLog's capacity
        """
        return self._capacity

    def add_entry(self, level, message):
        """
        Adds a log entry
        """
        if not (level == MessageLog.INFO or level == MessageLog.WARN or level == MessageLog.ALERT):
            raise ValueError

        timeinfo = datetime.datetime.now()
        log_message = (
            "%04d-%02d-%02dT%02d:%02d:%02dZ %s"
            % (timeinfo.year, timeinfo.month, timeinfo.day,
               timeinfo.hour, timeinfo.minute, timeinfo.second, str(message))
        )

        self._log_entries.append([level, log_message])

    def clear(self):
        """
        Clears all log entries
        """

        self._log_entries.clear()

    def iterate_entries(self):
        """
        Iterates over all log entries
        """
        return self._log_entries.__iter__()
