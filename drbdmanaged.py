#!/usr/bin/python

from drbdmanage.server import *

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 5:21:53 PM$"

def main():
    server      = DrbdManageServer()
    dbus_server = DBusServer(server)
    try:
        dbus_server.run()
    except KeyboardInterrupt:
        sys.stdout.write("Server shutdown (received SIGINT)\n")
    return 0

if __name__ == "__main__":
    main()
