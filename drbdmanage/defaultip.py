#!/usr/bin/env python2

import fcntl
import socket
import struct


def get_ip_address(ifname):
    """Returns the IP address of the specified interface"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # 0x8915 is SIOCGIFADDR
    return socket.inet_ntoa(
        fcntl.ioctl(sock.fileno(), 0x8915,
                    struct.pack('256s', ifname[:15]))[20:24]
    )


def get_interface_of_default_route():
    """Reads the default gateway directly from /proc"""
    interface = None
    try:  # might fail because '/proc/net/route' does not exist.
        with open("/proc/net/route") as fh:
            for line in fh:
                fields = line.strip().split()
                if fields[1] == '00000000' and int(fields[3], 16) & 2 != 0:
                    interface = fields[0]
    except Exception:  # catch all, IOError is the most likely one (which is very unlikely) ;-)
        pass

    return interface


def default_ip():
    """Returns the IP address of the default route interface"""
    ipaddress = None
    interface = get_interface_of_default_route()
    if interface is not None:
        ipaddress = get_ip_address(interface)
    return ipaddress


if __name__ == "__main__":
    print(default_ip())
