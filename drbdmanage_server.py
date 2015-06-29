#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013, 2014   LINBIT HA-Solutions GmbH
                               Author: R. Altnoeder

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

"""
Initialize the drbdmanage server and its D-Bus communication layer
"""

import logging
import drbdmanage.server
import drbdmanage.dbusserver


def main():
    """
    Starts up the server and its communication layer
    """
    server = drbdmanage.server.DrbdManageServer()
    drbdmanage.dbusserver.DBusServer(server)
    try:
        server.run()
    except KeyboardInterrupt:
        logging.info("server shutdown (received SIGINT)")
    return 0


if __name__ == "__main__":
    main()
