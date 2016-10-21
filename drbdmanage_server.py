#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013 - 2016  LINBIT HA-Solutions GmbH
                               Author: R. Altnoeder

    For further information see the COPYING file.
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
    signal_factory = drbdmanage.dbusserver.DBusSignalFactory()
    server = drbdmanage.server.DrbdManageServer(signal_factory)
    drbdmanage.dbusserver.DBusServer(server)
    try:
        server.run()
    except KeyboardInterrupt:
        logging.info("Server shutdown (received SIGINT)")
    return 0


if __name__ == "__main__":
    main()
