#!/usr/bin/python

from drbdmanage.server import *

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 5:21:53 PM$"


def main():
    """
    Starts up the server and its communication layer
    """
    server      = DrbdManageServer()
    dbus_server = DBusServer(server)
    try:
        server.run()
    except KeyboardInterrupt:
        sys.stdout.write("Server shutdown (received SIGINT)\n")
    return 0


if __name__ == "__main__":
    main()
