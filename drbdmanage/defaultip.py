#!/usr/bin/python

import socket
import fcntl
import struct
import time


def get_ip_address(ifname):
    """
    Returns the IP address of the specified interface
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # 0x8915 is SIOCGIFADDR
    return socket.inet_ntoa(
        fcntl.ioctl(sock.fileno(), 0x8915,
        struct.pack('256s', ifname[:15]))[20:24]
    )


def get_interface_of_default_route():
    """Read the default gateway directly from /proc."""
    with open("/proc/net/route") as fh:
        for line in fh:
            fields = line.strip().split()
            if fields[1] == '00000000' and int(fields[3], 16) & 2 != 0:
                return fields[0]


def default_ip():
    """
    Returns the IP address of the default route interface
    """
    interface = get_interface_of_default_route()
    return get_ip_address(interface) if interface else None


if __name__ == "__main__":
    print default_ip()
