#!/usr/bin/python

"""
Initialize the drbdmanage server and its D-Bus communication layer
"""

import logging
import drbdmanage.server
import drbdmanage.dbusserver

__author__ = "raltnoeder"
__date__   = "$Sep 12, 2013 5:21:53 PM$"


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
