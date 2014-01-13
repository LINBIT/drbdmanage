#!/usr/bin/python

import sys
import os
import os.path
import subprocess
import errno
from distutils.core import setup


def install(inst_path):
    """
    Install drbdmanage on the local machine
    """
    pass
    mkdir(inst_path + "var/lib/drbdmanage")
    mkdir(inst_path + "var/drbd.d")
    mkdir(inst_path + "etc/drbd.d")
    install_file(inst_path, "conf/drbdmanage-resources.res",
      "etc/drbd.d/")
    install_file(inst_path, "conf/drbdctrl.res",
      "etc/drbd.d/")
    install_file(inst_path, "conf/drbdmanaged.conf",
      "etc/")
    install_file(inst_path, "conf/drbdmanaged-lvm.conf",
      "etc/")
    # TODO: should an empty template of the LVM state file be installed?
    # install_file(inst_path, "conf/drbdmanaged-lvm.local.json",
    #   "var/lib/drbdmanage/")
    mkdir(inst_path + "etc/dbus-1/system.d")
    install_file(inst_path, "conf/org.drbd.drbdmanaged.conf",
      "etc/dbus-1/system.d/")
    mkdir(inst_path + "usr/bin/drbdmanage")
    install_dir(inst_path, "drbdmanage", "usr/bin/")
    install_file(inst_path, "drbdmanage.py", "usr/bin/")
    install_file(inst_path, "drbdmanaged.py", "usr/bin")
    install_file(inst_path, "drbdctrl_init.py", "usr/bin")
    
    #setup(
    #  name='drbdmanage',
    #  version='0.10',
    #  description='DRBD distributed resource management utility',
    #  author='LINBIT',
    #  author_email='office@linbit.com',
    #  url='http://oss.linbit.com',
    #  packages=[
    #    'drbdmanage',
    #    'drbdmanage.conf',
    #    'drbdmanage.drbd',
    #    'drbdmanage.storage'
    #  ]
    #)


def install_file(path, source, dest):
    """
    Call Unix's cp utility to copy a file into its installation directory
    """
    rc = subprocess.call(["cp", source, path + dest])
    if rc != 0:
        install_error_exit(
          "Cannot install file %s to %s\n"
          % (source, path + dest)
        )


def install_dir(path, source, dest):
    """
    Call Unix's cp utility to recursively copy a directory to its
    installation directory
    """
    rc = subprocess.call(["cp", "-r", source, path + dest])
    if rc != 0:
        install_error_exit(
          "Cannot install directory %s to %s\n"
          % (source, path + dest)
        )


def mkdir(path):
    """
    Create all subdirectories until the specified path is established
    """
    path_tokens = path.split("/")
    if path.startswith("/"):
        dir_path = ""
    else:
        dir_path = "."
    for dir in path_tokens:
        if len(dir) == 0 or dir == ".":
            continue
        dir_path += "/" + dir
        if os.access(dir_path, os.F_OK) == False:
            try:
                os.mkdir(dir_path, 0775)
            except OSError as oserr:
                if oserr.errno == errno.EACCES:
                    install_error_exit(
                      "Cannot create directory %s: permission denied\n"
                      % (dir_path)
                    )
                else:
                    install_error_exit(
                      "Cannot create directory %s: "
                      "the OS returned the following error:\n"
                      "%s (errno=%d)\n"
                      % (dir_path, oserr.strerror, oserr.errno)
                    )


def install_error_exit(error_msg):
    """
    Abort installation with an error message on stderr and exit
    with exit code 1
    """
    sys.stderr.write(error_msg)
    sys.stderr.write("Installation aborted\n")
    exit(1)


def main():
    """
    Process installation options and call the install function with a
    sanitized prefix path
    """
    myname = "setup.py"
    prefix = None
    if len(sys.argv) >= 1:
        myname = sys.argv[0]
    while True:
        arg = next_arg(sys.argv)
        if arg is None:
            break
        if arg == "--prefix":
            arg = next_arg(sys.argv)
            if arg is not None:
                prefix = arg
            else:
                syntax_exit(myname)
        else:
            syntax_exit(myname)
    if prefix is None:
        inst_path = "/"
    else:
        inst_path = os.path.realpath(prefix) + "/"
    install(inst_path)
    sys.stdout.write("Installation completed successfully.\n")


def next_arg(argv):
    """
    Returns the next argument each time it is called, or None if there are
    no more arguments
    """
    global arg_idx
    global arg_len
    arg = None
    if arg_idx < arg_len:
        arg = argv[arg_idx]
        arg_idx += 1
    return arg


def syntax_exit(myname):
    """
    Explain the installer's syntax on stderr and exit with exit code 1
    """
    sys.stderr.write("Syntax: %s [ --prefix <path> ]\n" % (myname))
    exit(1)


arg_idx = 1
arg_len = len(sys.argv)

if __name__ == "__main__":
    main()
