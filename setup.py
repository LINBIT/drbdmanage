#!/usr/bin/python
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
Installer & Package creator for drbdmanage
"""

import os, glob

from distutils.core import setup, Command


def get_version():
    from drbdmanage.consts import DM_VERSION
    return DM_VERSION


class CheckUpToDate(Command):
    description = "Check if version strings are up to date"
    user_options = []

    def initialize_options(self):
        self.cwd = None

    def finalize_options(self):
        self.cwd = os.getcwd()

    def run(self):
        import sys
        version = get_version()
        with open("debian/changelog") as f:
            firstline = f.readline()
            if version not in firstline:
                # returning false is not promoted
                sys.exit(1)


class BuildManCommand(Command):

    """
    Builds manual pages using docbook
    """

    description  = "Build manual pages"
    user_options = []


    def initialize_options(self):
        self.cwd = None


    def finalize_options(self):
        self.cwd = os.getcwd()


    def run(self):
        assert os.getcwd() == self.cwd, "Must be in package root: %s" % self.cwd
        outdir = "man-pages"
        if not os.path.isfile(os.path.join(outdir, "drbdmanage.8.gz")):
            os.system("cd %s; " % (outdir) + ' ' +
                "xsltproc --xinclude --stringparam variablelist.term.break.after 1 "
                "http://docbook.sourceforge.net/release/xsl/current/manpages/docbook.xsl "
                "drbdmanage.xml; gzip -f -9 drbdmanage.8")
        # subcommands
        import subprocess
        import gzip

        from drbdmanage_client import DrbdManage

        name = "dm"
        mansection = '8'
        replace = ("drbdmanage_client.py", "drbdmanage")

        client = DrbdManage()
        for cmd in client._all_commands:
            toplevel = cmd[0]
            # aliases = cmd[1:]
            # we could use the aliases to symlink them to the toplevel cmd
            outfile = os.path.join('.', outdir, name + '-' + toplevel + '.' +
                                   mansection + ".gz")
            if os.path.isfile(outfile):
                continue
            print "Generating %s ..." % (outfile)
            mangen = ["help2man", "-n", toplevel, '-s', '8',
                      '--version-string=%s' % (get_version()), "-N",
                      '"./drbdmanage_client.py %s"' % (toplevel)]

            toexec = " ".join(mangen)
            manpage = subprocess.check_output(toexec, shell=True)
            manpage = manpage.replace(replace[0], replace[1])
            manpage = manpage.replace(replace[0].upper(), replace[1].upper())
            with gzip.open(outfile, 'wb') as f:
                f.write(manpage)


def gen_data_files():
    data_files = [("/etc/drbd.d", ["conf/drbdctrl.res_template",
                                   "conf/drbdmanage-resources.res"]),
                  ("/etc", ["conf/drbdmanaged.conf",
                            "conf/drbdmanaged-lvm.conf"]),
                  ("/etc/dbus-1/system.d", ["conf/org.drbd.drbdmanaged.conf"]),
                  ("/usr/share/dbus-1/system-services",
                   ["conf/org.drbd.drbdmanaged.service"]),
                  ("/var/lib/drbd.d", []),
                  ("/var/lib/drbdmanage", []),
                  ("/etc/bash_completion.d", ["scripts/bash_completion/drbdmanage"])]

    for manpage in glob.glob(os.path.join("man-pages", "*.8.gz")):
        data_files.append(("/usr/share/man/man8", [manpage]))

    return data_files

setup(
    name="drbdmanage",
    version=get_version(),
    description="DRBD distributed resource management utility",
    long_description=
"Drbdmanage is a daemon and a command line utility that manages DRBD\n" +
"replicated LVM volumes across a group of machines.\n" +
"It maintains DRBD configuration an the participating machines. It\n" +
"creates/deletes the backing LVM volumes. It automatically places\n" +
"the backing LVM volumes among the participating machines.",
    author="Robert Altnoeder",
    author_email="robert.altnoeder@linbit.com",
    maintainer="LINBIT HA Solutions GmbH",
    maintainer_email="drbd-dev@lists.linbit.com",
    url="http://oss.linbit.com/drbdmanage",
    license="GPLv3",
    packages=[
        "drbdmanage",
        "drbdmanage.conf",
        "drbdmanage.drbd",
        "drbdmanage.storage",
        "drbdmanage.snapshots",
        "drbdmanage.argparse",
        "drbdmanage.argcomplete"],
    py_modules=["drbdmanage_server", "drbdmanage_client"],
    scripts=["scripts/drbdmanage", "scripts/dbus-drbdmanaged-service"],
    data_files=gen_data_files(),
    cmdclass={
        "build_man": BuildManCommand,
        "versionup2date": CheckUpToDate
        }
    )
