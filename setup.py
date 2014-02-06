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

from distutils.core import setup, Command
import os

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
        os.system("cd man-pages; "
            "xsltproc --xinclude --stringparam variablelist.term.break.after 1 "
            "http://docbook.sourceforge.net/release/xsl/current/manpages/docbook.xsl "
            "drbdmanage.xml")

setup(
    name="drbdmanage",
    version="0.10",
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
        "drbdmanage.storage"],
    py_modules=["drbdmanage_server", "drbdmanage_client"],
    scripts=["scripts/drbdmanage", "scripts/dbus-drbdmanaged-service"],
    data_files=[("/etc/drbd.d", ["conf/drbdctrl.res_template",
                    "conf/drbdmanage-resources.res"]),
                ("/etc", ["conf/drbdmanaged.conf",
                    "conf/drbdmanaged-lvm.conf"]),
                ("/etc/dbus-1/system.d", ["conf/org.drbd.drbdmanaged.conf"]),
                ("/usr/share/man", ["man-pages/drbdmanage.8"]),
                ("/usr/share/dbus-1/system-services",
                    ["conf/org.drbd.drbdmanaged.service"]),
                ("/var/drbd.d", []),
                ("/var/lib/drbdmanage", [])
               ],
    cmdclass={
        "build_man": BuildManCommand
        }
    )
