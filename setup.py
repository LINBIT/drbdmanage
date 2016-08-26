#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013 - 2016  LINBIT HA-Solutions GmbH
                               Author: R. Altnoeder, Roland Kammerer

    For further information see the COPYING file.
"""

"""
Installer & Package creator for drbdmanage
"""

import os
import glob
import sys

from distutils.core import setup, Command


def get_version():
    from drbdmanage.consts import DM_VERSION
    return DM_VERSION


# used to overwrite version tag by internal build tools
# keep it, even if you don't understand it.
def get_setup_version():
    return get_version()


class CheckUpToDate(Command):
    description = "Check if version strings are up to date"
    user_options = []

    def initialize_options(self):
        self.cwd = None

    def finalize_options(self):
        self.cwd = os.getcwd()

    def run(self):
        version = get_version()
        try:
            with open("debian/changelog") as f:
                firstline = f.readline()
                if version not in firstline:
                    # returning false is not promoted
                    sys.exit(1)
        except IOError:
            # probably a release tarball without the debian directory but with Makefile
            return True


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
        from drbdmanage_client import DrbdManage
        outdir = "man-pages"
        name = "drbdmanage"
        mansection = '8'
        client = DrbdManage()
        descriptions = client.parser_cmds_description(client._all_commands)

        if not os.path.isfile(os.path.join(outdir, "drbdmanage.8.gz")):
            h = open(os.path.join(outdir, "drbdmanage_header.xml"))
            t = open(os.path.join(outdir, "drbdmanage_trailer.xml"))
            drbdmanagexml = open(os.path.join(outdir, "drbdmanage.xml"), 'w')
            drbdmanagexml.write(h.read())
            for cmd in [cmds[0] for cmds in client._all_commands]:
                drbdmanagexml.write("""
                <varlistentry>
                  <term>
                      <command moreinfo="none">drbdmanage</command>
                      <arg choice="plain" rep="norepeat">%s
                      </arg>
                  </term>
                  <listitem>
                    <para>
                       %s
                    </para>
                    <para>For furter information see
                        <citerefentry>
                        <refentrytitle>%s</refentrytitle>
                        <manvolnum>%s</manvolnum></citerefentry>
                    </para>
                  </listitem>
                </varlistentry>
                """ % (cmd, descriptions[cmd], name + '-' + cmd, mansection))
            drbdmanagexml.write(t.read())
            h.close()
            t.close()
            drbdmanagexml.close()

            os.system("cd %s; " % (outdir) + ' ' +
                      "xsltproc --xinclude --stringparam variablelist.term.break.after 1 "
                      "http://docbook.sourceforge.net/release/xsl/current/manpages/docbook.xsl "
                      "drbdmanage.xml; gzip -f -9 drbdmanage.8")
        # subcommands
        import gzip
        if "__enter__" not in dir(gzip.GzipFile):  # duck punch it in!
            def __enter(self):
                if self.fileobj is None:
                    raise ValueError("I/O operation on closed GzipFile object")
                return self

            def __exit(self, *args):
                self.close()

            gzip.GzipFile.__enter__ = __enter
            gzip.GzipFile.__exit__ = __exit

        from drbdmanage.utils import check_output

        replace = ("drbdmanage_client.py", "drbdmanage")

        for cmd in client._all_commands:
            toplevel = cmd[0]
            # aliases = cmd[1:]
            # we could use the aliases to symlink them to the toplevel cmd
            outfile = os.path.join('.', outdir, name + '-' + toplevel + '.' +
                                   mansection + ".gz")
            if os.path.isfile(outfile):
                continue
            sys.stdout.write("Generating %s ...\n" % (outfile))
            mangen = ["help2man", "-n", toplevel, '-s', mansection,
                      '--version-string=%s' % (get_version()), "-N",
                      '"./drbdmanage_client.py %s"' % (toplevel)]

            toexec = " ".join(mangen)
            manpage = check_output(toexec, shell=True)
            manpage = manpage.replace(replace[0], replace[1])
            manpage = manpage.replace(replace[0].upper(), replace[1].upper())
            manpage = manpage.replace(toplevel.upper(), mansection)
            manpage = manpage.replace("%s %s" % (replace[1], toplevel),
                                      "%s_%s" % (replace[1], toplevel))
            with gzip.open(outfile, 'wb') as f:
                f.write(manpage)


def gen_data_files():
    data_files = [("/etc/drbd.d", ["conf/drbdctrl.res_template",
                                   "conf/drbdmanage-resources.res"]),
                  ("/etc/dbus-1/system.d", ["conf/org.drbd.drbdmanaged.conf"]),
                  ("/usr/share/dbus-1/system-services",
                   ["conf/org.drbd.drbdmanaged.service"]),
                  ("/lib/systemd/system/", ["conf/drbdmanaged.service"]),
                  ("/var/lib/drbd.d", []),
                  ("/var/lib/drbdmanage", []),
                  ("/etc/bash_completion.d", ["scripts/bash_completion/drbdmanage"])]

    # Don't overwrite existing file
    if not os.path.isfile("/etc/drbdmanaged.cfg"):
        data_files.append( ("/etc", ["conf/drbdmanaged.cfg"]) )

    for manpage in glob.glob(os.path.join("man-pages", "*.8.gz")):
        data_files.append(("/usr/share/man/man8", [manpage]))

    return data_files

setup(
    name="drbdmanage",
    version=get_setup_version(),
    description="DRBD distributed resource management utility",
    long_description="Drbdmanage is a daemon and a command line utility that manages DRBD\n" +
    "replicated LVM volumes across a group of machines.\n" +
    "It maintains DRBD configuration an the participating machines. It\n" +
    "creates/deletes the backing LVM volumes. It automatically places\n" +
    "the backing LVM volumes among the participating machines.",
    author="Robert Altnoeder <robert.altnoeder@linbit.com>, Roland Kammerer <roland.kammerer@linbit.com>",
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
        "drbdmanage.argcomplete",
        "drbdmanage.importlib",
        "drbdmanage.plugins",
        "drbdmanage.plugins.plugins",
        "drbdmanage.plugins.external",
    ],
    py_modules=["drbdmanage_server", "drbdmanage_client"],
    scripts=["scripts/drbdmanage", "scripts/dbus-drbdmanaged-service"],
    data_files=gen_data_files(),
    cmdclass={
        "build_man": BuildManCommand,
        "versionup2date": CheckUpToDate
        }
    )
