#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013 - 2017  LINBIT HA-Solutions GmbH
                               Author: R. Altnoeder, Roland Kammerer

    You can use this file under the terms of the GNU Lesser General
    Public License as as published by the Free Software Foundation,
    either version 3 of the License, or (at your option) any later
    version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    See <http://www.gnu.org/licenses/>.
"""

"""
Generalized utility functions and classes for drbdmanage
"""

import dbus
import errno
import os
import sys
import hashlib
import base64
import operator
import subprocess
import select
import fcntl
import errno
import uuid
import drbdmanage.consts as consts
import logging
import locale
import pickle
import copy_reg
import ConfigParser
from functools import wraps
from drbdmanage.exceptions import SyntaxException, InvalidNameException, EventException
from drbdmanage.consts import (
    SERVER_CONFFILE, PLUGIN_PREFIX, KEY_DRBD_CONFPATH, KEY_DRBDCTRL_VG,
    KEY_SAT_CFG_ROLE, KEY_COLORS, KEY_UTF8, RES_NAME, SNAPS_NAME, NODE_NAME,
    KEY_LOGLEVEL, NODE_NAME_MINLEN, NODE_NAME_MAXLEN, NODE_NAME_LABEL_MAXLEN,
    RES_NAME_MINLEN, RES_NAME_MAXLEN, SNAPS_NAME_MINLEN, SNAPS_NAME_MAXLEN,
    RES_NAME_VALID_CHARS, SNAPS_NAME_VALID_CHARS, RES_NAME_VALID_INNER_CHARS,
    SNAPS_NAME_VALID_INNER_CHARS
)

from drbdmanage.exceptions import (
    DM_ENOTREADY, DM_ENOTREADY_STARTUP, DM_ENOTREADY_REQCTRL
)

COLOR_BLACK     = chr(0x1b) + "[0;30m"
COLOR_DARKRED   = chr(0x1b) + "[0;31m"
COLOR_DARKGREEN = chr(0x1b) + "[0;32m"
COLOR_BROWN     = chr(0x1b) + "[0;33m"
COLOR_DARKBLUE  = chr(0x1b) + "[0;34m"
COLOR_DARKPINK  = chr(0x1b) + "[0;35m"
COLOR_TEAL      = chr(0x1b) + "[0;36m"
COLOR_GRAY      = chr(0x1b) + "[0;37m"
COLOR_DARKGRAY  = chr(0x1b) + "[1;30m"
COLOR_RED       = chr(0x1b) + "[1;31m"
COLOR_GREEN     = chr(0x1b) + "[1;32m"
COLOR_YELLOW    = chr(0x1b) + "[1;33m"
COLOR_BLUE      = chr(0x1b) + "[1;34m"
COLOR_PINK      = chr(0x1b) + "[1;35m"
COLOR_TURQUOIS  = chr(0x1b) + "[1;36m"
COLOR_WHITE     = chr(0x1b) + "[1;37m"
COLOR_NONE      = chr(0x1b) + "[0m"

# Length of random data for generating shared secrets
SECRET_LEN    = 15

# Default terminal dimensions
# Used by get_terminal_size()
DEFAULT_TERM_WIDTH  = 80
DEFAULT_TERM_HEIGHT = 25


class Table():
    def __init__(self, colors=True, utf8=False, pastable=False):
        self.r_just = False
        self.got_column = False
        self.got_row = False
        self.groups = []
        self.header = []
        self.table = []
        self.coloroverride = []
        self.view = None
        self.showseps = False
        self.maxwidth = 0  # if 0, determine terminal width automatically
        if pastable:
            self.colors = False
            self.utf8 = False
            self.maxwidth = 78
        else:
            self.colors = colors
            self.utf8 = utf8

    def add_column(self, name, color=False, just_col='<', just_txt='<'):
        self.got_column = True
        if self.got_row:
            raise SyntaxException("Not allowed to define columns after rows")
        if just_col == '>':
            if self.r_just:
                raise SyntaxException("Not allowed to use multiple times")
            else:
                self.r_just = True

        self.header.append({
            'name': name,
            'color': color,
            'just_col': just_col,
            'just_txt': just_txt})

    def add_row(self, row, color=False, just_txt='<'):
        self.got_row = True
        if not self.got_column:
            raise SyntaxException("Not allowed to define rows before columns")
        if len(row) != len(self.header):
            raise SyntaxException("Row len does not match headers")

        coloroverride = [None] * len(row)
        for idx, c in enumerate(row[:]):
            if isinstance(c, tuple):
                color, text = c
                row[idx] = text
                if self.colors:
                    if not self.header[idx]['color']:
                        raise SyntaxException("Color tuple for this row not allowed "
                                              "to have colors")
                    else:
                        coloroverride[idx] = color

        self.table.append(row)
        self.coloroverride.append(coloroverride)

    def add_separator(self):
        self.table.append([None])

    def set_show_separators(self, val=False):
        self.showseps = val

    def set_view(self, columns):
        self.view = columns

    def set_groupby(self, groups):
        if groups:
            self.groups = groups

    def show(self, machine_readable=False, overwrite=False):
        if machine_readable:
            overwrite = False

        # no view set, use all headers
        if not self.view:
            self.view = [h['name'] for h in self.header]

        if self.groups:
            self.view += [g for g in self.groups if g not in self.view]

        pcnt = 0
        for idx, c in enumerate(self.header[:]):
            if c['name'] not in self.view:
                pidx = idx - pcnt
                pcnt += 1
                self.header.pop(pidx)
                for row in self.table:
                    row.pop(pidx)
                for row in self.coloroverride:
                    row.pop(pidx)

        columnmax = [0] * len(self.header)
        if self.maxwidth:
            maxwidth = self.maxwidth
        else:
            term_width, _ = get_terminal_size()
            maxwidth = 110 if term_width > 110 else term_width

        # color overhead
        co = len(COLOR_RED) + len(COLOR_NONE)
        co_sum = 0

        hdrnames = [h['name'] for h in self.header]
        if self.groups:
            group_bys = [hdrnames.index(g) for g in self.groups if g in hdrnames]
            for row in self.table:
                for idx in group_bys:
                    try:
                        row[idx] = int(row[idx])
                    except ValueError:
                        pass
            orig_table = self.table[:]
            orig_coloroverride = self.coloroverride[:]
            tidx_used = set()
            try:
                from natsort import natsorted
                self.table = natsorted(self.table, key=operator.itemgetter(*group_bys))
            except:
                self.table.sort(key=operator.itemgetter(*group_bys))

            # restore color overrides after sort
            for oidx, orow in enumerate(orig_table):
                for tidx, trow in enumerate(self.table):
                    if orow == trow and oidx != tidx and tidx not in tidx_used:
                        tidx_used.add(tidx)
                        self.coloroverride[tidx] = orig_coloroverride[oidx]
                        break

            lstlen = len(self.table)
            seps = set()
            for c in range(len(self.header)):
                if c not in group_bys:
                    continue
                cur = self.table[0][c]
                for idx, l in enumerate(self.table):
                    if idx < lstlen - 1:
                        if self.table[idx + 1][c] == cur:
                            if overwrite:
                                self.table[idx + 1][c] = ' '
                        else:
                            cur = self.table[idx + 1][c]
                            seps.add(idx + 1)

            if self.showseps:
                for c, pos in enumerate(sorted(seps)):
                    self.table.insert(c + pos, [None])

        # calc max width per column and set final strings (with color codes)
        self.table.insert(0, [h.replace('_', ' ') for h in hdrnames])
        ridx = 0
        self.coloroverride.insert(0, [None] * len(self.header))

        for row in self.table:
            if not row[0]:
                continue
            for idx, col in enumerate(self.header):
                row[idx] = str(row[idx])
                if col['color']:
                    if self.coloroverride[ridx][idx]:
                        color = self.coloroverride[ridx][idx]
                    else:
                        color = col["color"]
                    row[idx] = color + row[idx] + COLOR_NONE
                columnmax[idx] = max(len(row[idx]), columnmax[idx])
            ridx += 1

        for h in self.header:
            if h['color']:
                co_sum += co

        # insert frames
        self.table.insert(0, [None])
        self.table.insert(2, [None])
        self.table.append([None])

        # build format string
        ctbl = {
            'utf8': {
                'tl': '╭',   # top left
                'tr': '╮',   # top right
                'bl': '╰',   # bottom left
                'br': '╯',   # bottom right
                'mr': '╡',   # middle right
                'ml': '╞',   # middle left
                'mdc': '┄',  # middle dotted connector
                'msc': '─',  # middle straight connector
                'pipe': '┊'
            },
            'ascii': {
                'tl': '+',
                'tr': '+',
                'bl': '+',
                'br': '+',
                'mr': '|',
                'ml': '|',
                'mdc': '-',
                'msc': '-',
                'pipe': '|'
            }
        }

        enc = 'ascii'
        try:
            import locale
            if locale.getdefaultlocale()[1].lower() == 'utf-8' and self.utf8:
                enc = 'utf8'
        except:
            pass

        fstr = ctbl[enc]['pipe']
        for idx, col in enumerate(self.header):
            if col['just_col'] == '>':
                space = (maxwidth - sum(columnmax) + co_sum)
                space_and_overhead = space - (len(self.header) * 3) - 2
                if space_and_overhead >= 0:
                    fstr += ' ' * space_and_overhead + ctbl[enc]['pipe']

            fstr += ' {' + str(idx) + ':' + col['just_txt'] + str(columnmax[idx]) + '} ' + ctbl[enc]['pipe']

        try:
            for idx, row in enumerate(self.table):
                if not row[0]:  # print a separator
                    if idx == 0:
                        l, m, r = ctbl[enc]['tl'], ctbl[enc]['msc'], ctbl[enc]['tr']
                    elif idx == len(self.table) - 1:
                        l, m, r = ctbl[enc]['bl'], ctbl[enc]['msc'], ctbl[enc]['br']
                    else:
                        l, m, r = ctbl[enc]['ml'], ctbl[enc]['mdc'], ctbl[enc]['mr']
                    sep = l + m * (sum(columnmax) - co_sum + (3 * len(self.header)) - 1) + r
                    if enc == 'utf8':  # should be save on non utf-8 too...
                        sep = sep.decode('utf-8')

                    if self.r_just and len(sep) < maxwidth:
                        sys.stdout.write(l + m * (maxwidth - 2) + r + "\n")
                    else:
                        sys.stdout.write(sep + "\n")
                else:
                    sys.stdout.write(fstr.format(*row) + "\n")
        except IOError as e:
            if e.errno == errno.EPIPE:
                return
            else:
                raise


# used by clienthelper and the cli. used during startup to check if the server is actually ready
def is_rc_retry(server_rc):
    for rc_entry in server_rc:
        rc_num, _, _ = rc_entry
        if rc_num == DM_ENOTREADY or rc_num == DM_ENOTREADY_STARTUP or rc_num == DM_ENOTREADY_REQCTRL:
            return True
    return False


# a wrapper for subprocess.check_output
def check_output(*args, **kwargs):
    def _wrapcall_2_6(*args, **kwargs):
        # no check_output in 2.6
        if "stdout" in kwargs:
            raise ValueError("stdout argument not allowed, it will be overridden.")
        process = subprocess.Popen(stdout=subprocess.PIPE, *args, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = args[0]
            raise subprocess.CalledProcessError(retcode, cmd)
        return output

    try:
        return subprocess.check_output(*args, **kwargs)
    except AttributeError:
        return _wrapcall_2_6(*args, **kwargs)


# a wrapper for subprocess call for one-shot commands that are not critical and where we don't care about
# stdout/stderr
def cmd_try_ignore(cmdlst):
    try:
        devnull = open(os.devnull, 'w')
        subprocess.call(cmdlst, stdout=devnull, stderr=subprocess.STDOUT)
    except:
        pass


def wipefs(device):
    cmd_try_ignore(["wipefs", "-a", device])


def ssh_exec(cmdname, ip, name, cmdline, quiet=False, suppress_stderr=False):
    try:
        ssh_base = ["ssh", "-oBatchMode=yes",
                    "-oConnectTimeout=2", "root@" + ip]
        if subprocess.call(ssh_base + ["true"]) == 0:
            sys.stdout.write(
                "\nExecuting %s command using ssh.\n"
                "IMPORTANT: The output you see comes from %s\n"
                "IMPORTANT: Your input is executed on %s\n"
                % (cmdname, name, name)
            )
            ssh_cmd = ssh_base + cmdline
            if quiet:
                ssh_cmd.append("-q")
            if suppress_stderr:
                ssh_cmd.append('2>/dev/null')
            subprocess.check_call(ssh_cmd)
            return True
    except subprocess.CalledProcessError:
        sys.stderr.write("Error: Attempt to execute the %s command remotely"
                         "failed\n" % (cmdname))

    return False


def drbdctrl_has_primary():
    try:
        call_str = '%s events2 --now %s | grep role:Primary' % (consts.DRBDSETUP_UTIL,
                                                                consts.DRBDCTRL_RES_NAME)
        output = check_output(call_str, shell=True)  # raise if rc != 0
        output = output.strip()
        for i in output.split():
            kv = i.split(':')
            if len(kv) == 2 and kv[0] == 'conn-name':
                return True, kv[1]
        return True, ''
    except:
        pass

    return False, ''


# base range check
def checkrange(v, i, j):
    return i <= v <= j


# "type" used for argparse
def rangecheck(i, j):
    def range(v):
        import argparse.argparse as argparse
        v = int(v)
        if not checkrange(v, i, j):
            raise argparse.ArgumentTypeError('Range: [%d, %d]' % (i, j))
        return v
    return range


def check_name(name, min_length, max_length, valid_chars, valid_inner_chars):
    """
    Check the validity of a string for use as a name for
    objects like nodes or volumes.
    A valid name must match these conditions:
      * must at least be 1 byte long
      * must not be longer than specified by the caller
      * contains a-z, A-Z, 0-9, and the characters allowed
        by the caller only
      * contains at least one alpha character (a-z, A-Z)
      * must not start with a numeric character
      * must not start with a character allowed by the caller as
        an inner character only (valid_inner_chars)
    @param name         the name to check
    @param max_length   the maximum permissible length of the name
    @param valid_chars  list of characters allowed in addition to
                        [a-zA-Z0-9]
    @param valid_inner_chars    list of characters allowed in any
                        position in the name other than the first,
                        in addition to [a-zA-Z0-9] and the characters
                        already specified in valid_chars
    returns a valid string or "" (which can be if-checked)
    """
    checked_name = None
    if min_length is None or max_length is None:
        raise ValueError
    if name is None:
        raise InvalidNameException
    name_b = bytearray(str(name), "utf-8")
    name_len = len(name_b)
    if name_len < min_length or name_len > max_length:
        raise InvalidNameException
    alpha = False
    idx = 0
    while idx < name_len:
        item = name_b[idx]
        if item >= ord('a') and item <= ord('z'):
            alpha = True
        elif item >= ord('A') and item <= ord('Z'):
            alpha = True
        else:
            if (not (item >= ord('0') and item <= ord('9') and idx >= 1)):
                letter = chr(item)
                if (not (letter in valid_chars or (letter in valid_inner_chars and idx >= 1))):
                    # Illegal character in name
                    raise InvalidNameException
        idx += 1
    if not alpha:
        raise InvalidNameException
    checked_name = str(name_b)
    return checked_name


def check_node_name(name):
    """
    RFC952 / RFC1123 internet host name validity check

    @returns Valid host name or raises drbdmanage.exceptions.InvalidNameException
    """
    if name is None:
        raise InvalidNameException
    name_b = bytearray(str(name), "utf-8")
    name_len = len(name_b)
    if name_len < NODE_NAME_MINLEN or name_len > NODE_NAME_MAXLEN:
        raise InvalidNameException
    for label in name_b.split("."):
        if len(label) > NODE_NAME_LABEL_MAXLEN:
            raise InvalidNameException
    idx = 0
    while idx < name_len:
        letter = name_b[idx]
        if not ((letter >= ord('a') and letter <= ord('z')) or
            (letter >= ord('A') and letter <= ord('Z')) or
            (letter >= ord('0') and letter <= ord('9'))):
            # special characters allowed depending on position within the string
            if idx == 0 or idx + 1 == name_len:
                raise InvalidNameException
            else:
                if not (letter == ord('.') or letter == ord('-')):
                    raise InvalidNameException
        idx += 1
    checked_name = str(name_b)
    return checked_name


# "type" used for argparse
def namecheck(checktype):
    # Define variables in this scope (Python 3.x compatibility)
    min_length = 0
    max_length = 0
    valid_chars = ""
    valid_inner_chars = ""

    if checktype == RES_NAME:
        min_length = RES_NAME_MINLEN
        max_length = RES_NAME_MAXLEN
        valid_chars = RES_NAME_VALID_CHARS
        valid_inner_chars = RES_NAME_VALID_INNER_CHARS
    elif checktype == SNAPS_NAME:
        min_length = SNAPS_NAME_MINLEN
        max_length = SNAPS_NAME_MAXLEN
        valid_chars = SNAPS_NAME_VALID_CHARS
        valid_inner_chars = SNAPS_NAME_VALID_INNER_CHARS
    else:  # checktype == NODE_NAME, use that as rather arbitrary default
        min_length = NODE_NAME_MINLEN
        max_length = NODE_NAME_MAXLEN

    def check(name):
        import argparse.argparse as argparse
        try:
            if checktype == NODE_NAME:
                name = check_node_name(name)
            else:
                name = check_name(name, min_length, max_length, valid_chars, valid_inner_chars)
        except InvalidNameException:
            raise argparse.ArgumentTypeError('Name: %s not valid' % (name))
        return name
    return check


def get_uname():
    checked_node_name = None
    try:
        node_name = None
        uname = os.uname()
        if len(uname) >= 2:
            node_name = uname[1]
        try:
            checked_node_name = check_node_name(node_name)
        except InvalidNameException:
            pass
    except OSError:
        pass
    return checked_node_name


def load_server_conf_file(localonly=False):
    """
    Try to load the server configuration.
    """
    cfgdict = {'local': {}, 'plugins': {}}

    cfg = ConfigParser.RawConfigParser()
    read_ok = cfg.read(SERVER_CONFFILE)  # read catches IOErrors internally, returns list of ok files
    if len(read_ok) == 1:
        for section in cfg.sections():
            if section.startswith('LOCAL'):
                in_file_cfg = dict(cfg.items(section))
                if not cfg.has_option(section, 'force'):
                    final_config = filter_allowed(in_file_cfg.copy(), (KEY_DRBDCTRL_VG,
                                                                       'extend-path',
                                                                       KEY_LOGLEVEL,
                                                                       KEY_SAT_CFG_ROLE,
                                                                       KEY_DRBD_CONFPATH,
                                                                       KEY_COLORS,
                                                                       KEY_UTF8))
                    ignored = [k for k in in_file_cfg if k not in final_config]
                    for k in ignored:
                        logging.warning('Ignoring %s in configuration file' % k)
                else:
                    final_config = in_file_cfg

                cfgdict['local'] = final_config

            elif section.startswith(PLUGIN_PREFIX):
                cfgdict['plugins'][section[len(PLUGIN_PREFIX):]] = dict(cfg.items(section))
    else:
        logging.warning("Could not read configuration file '%s'" % (SERVER_CONFFILE))

    if localonly:
        return cfgdict['local']
    else:
        return cfgdict


# mainly used for DrbdSetupOpts()
# but also usefull for 'handlers' subcommand
def filter_new_args(unsetprefix, args):
    new = dict()
    reserved_keys = ["func", "optsobj", "common", "command"]
    for k, v in args.__dict__.iteritems():
        if v is not None and k not in reserved_keys:
            key = k.replace('_', '-')

            # handle --unset
            if key.startswith(unsetprefix) and not v:
                continue

            strv = str(v)
            if strv == 'False':
                strv = 'no'
            if strv == 'True':
                strv = 'yes'

            new[key] = strv

    for k in new.keys():
        if "unset-" + k in new:
            sys.stderr.write('Error: You are not allowed to set and unset'
                             ' an option at the same time!\n')
            return False
    return new


class DrbdSetupOpts():
    def __init__(self, setup_command, dm_command=None):
        import sys
        import xml.etree.ElementTree as ET
        self.setup_command = setup_command
        self.dm_command = dm_command if dm_command else setup_command
        self.config = {}
        self.unsetprefix = 'unset'
        self.ok = False

        out = False
        for cmd in ('drbdsetup', '/sbin/drbdsetup'):
            try:
                out = check_output([cmd, "xml-help", self.setup_command])
                break
            except OSError:
                pass
        if not out:
            sys.stderr.write("Could not execute drbdsetup\n")
            return

        root = ET.fromstring(out)

        for child in root:
            if child.tag == 'summary':
                self.config['help'] = child.text
            elif child.tag == 'argument':
                # ignore them
                pass
            elif child.tag == 'option':
                opt = child.attrib['name']
                self.config[opt] = {'type': child.attrib['type']}
                if child.attrib['name'] == 'set-defaults':
                    continue
                if child.attrib['type'] == 'boolean':
                    self.config[opt]['default'] = child.find('default').text
                if child.attrib['type'] == 'handler':
                    self.config[opt]['handlers'] = [h.text for h in child.findall('handler')]
                elif child.attrib['type'] == 'numeric':
                    for v in ('min', 'max', 'default', 'unit_prefix', 'unit'):
                        val = child.find(v)
                        if val is not None:
                            self.config[opt][v] = val.text
        self.ok = True

    def genArgParseSubcommand(self, subp):
        sp = subp.add_parser(self.dm_command, description=self.config['help'])

        def mybool(x):
            return x.lower() in ('y', 'yes', 't', 'true', 'on')

        for opt in self.config:
            if opt == 'help':
                continue
            if self.config[opt]['type'] == 'handler':
                sp.add_argument('--' + opt, choices=self.config[opt]['handlers'])
            if self.config[opt]['type'] == 'boolean':
                sp.add_argument('--' + opt, type=mybool,
                                help="yes/no (Default: %s)" % (self.config[opt]['default']))
            if self.config[opt]['type'] == 'string':
                sp.add_argument('--' + opt)
            if self.config[opt]['type'] == 'numeric':
                min_ = int(self.config[opt]['min'])
                max_ = int(self.config[opt]['max'])
                default = int(self.config[opt]['default'])
                if "unit" in self.config[opt]:
                    unit = "; Unit: " + self.config[opt]['unit']
                else:
                    unit = ""
                # sp.add_argument('--' + opt, type=rangecheck(min_, max_),
                #                 default=default, help="Range: [%d, %d]; Default: %d" %(min_, max_, default))
                # setting a default sets the option to != None, which makes
                # filterNew relatively complex
                sp.add_argument('--' + opt, type=rangecheck(min_, max_),
                                help="Range: [%d, %d]; Default: %d%s" % (min_, max_, default, unit))
        for opt in self.config:
            if opt == 'help':
                continue
            else:
                sp.add_argument('--%s-%s' % (self.unsetprefix, opt),
                                action='store_true')

        return sp

    # return a dict containing all non-None args
    def filterNew(self, args):
        return filter_new_args(self.unsetprefix, args)

    # returns True if opt is a valid option and val has the correct type and
    # satisfies the specified check (e.g., a range check)
    def validateCommand(self, opt, val):
        if opt not in self.config:
            return False

        if self.config[opt]['type'] == 'handler':
            return val in self.config[opt]['handlers']
        if self.config[opt]['type'] == 'boolean':
            return type(val) == type(bool())
        if self.config[opt]['type'] == 'string':
            return type(val) == type(str())
        if self.config[opt]['type'] == 'numeric':
            min_ = int(self.config[opt]['min'])
            max_ = int(self.config[opt]['max'])
            return checkrange(val, min_, max_)

    def get_options(self):
        return self.config


def get_free_number(min_nr, max_nr, nr_list, nr_sorted=False):
    """
    Returns the first number in the range min_nr..max_nr that is not in nr_list

    In the range min_nr to max_nr, finds and returns a number that is not in the
    supplied list of numbers.
    min_nr and max_nr must be >= 0, and nr_list must be a list of integer numbers

    @param   min_nr:  range start, >= 0
    @param   max_nr:  range end, >= 0, greater than or equal to min_nr
    @param   nr_list: list of integer numbers
    @type    nr_list: list of int or long values
    @return: first free number within min_nr..max_nr; or -1 on error
    """
    if not nr_sorted:
        nr_list = sorted(nr_list)
    free_nr = -1
    if min_nr >= 0 and min_nr <= max_nr:
        nr_list_length = len(nr_list)
        index = 0
        number = min_nr
        while number <= max_nr and free_nr == -1:
            occupied = False
            while index < nr_list_length:
                if nr_list[index] >= number:
                    if nr_list[index] == number:
                        occupied = True
                    break
                index += 1
            if occupied:
                index += 1
            else:
                free_nr = number
            number += 1
    return free_nr


def fill_list(in_list, out_list, count):
    """
    Append items to a list until the list's length is (at least) equal to
    count.

    @param   in_list: List with items to append to out_list
    @param   out_list: List where items from in_list should be appended
    @param   count: Fill target for out_list
    """
    out_len = len(out_list)
    if out_len < count:
        out_len = count - out_len
        in_len  = len(in_list)
        idx     = 0
        max_len = out_len if out_len < in_len else in_len
        while idx < max_len:
            out_list.append(in_list[idx])
            idx += 1


def build_path(prefix, filename):
    """
    Builds a full path name from a prefix (commonly loaded from a configuration
    file) and a filename. If no prefix is specified, the full path will be
    just the filename to allow loading files from the current directory or
    from $PATH (if that is supported by whatever function requested the
    full path). If a prefix is specified, it can be a relative or absolute
    path with or without a trailing slash.

    @param   prefix: path to the file
    @type    prefix: str
    @param   filename: a file name to append to the path
    @type    filename: str
    @return: path assembled from prefix and file name
    @rtype:  str
    """
    if prefix is not None and len(prefix) > 0:
        return os.path.join(prefix, filename)
    else:
        return filename


def extend_path(orig_path, ext_path):
    """
    Extends the PATH environment variable exported to subprocesses

    Mainly required for D-Bus activation, because the D-Bus helper clears
    all environment variables, and most utilities that get called by
    drbdmanage require a sane PATH or else they fail.

    This function checks whether all the directories supplied in the ext_path
    arguments are already present in PATH, and for those that are not present
    yet, it appends each directory stated in ext_path to PATH in the order
    of occurrence in ext_path.
    """
    sep = ":"
    path_items = orig_path.split(sep)
    ext_path_items = ext_path.split(sep)

    path = [p for p in ext_path_items if p not in path_items] + path_items
    # this will implicitly update the actual PATH environment variable
    # by means of a call to putenv() according to python documentation
    os.environ["PATH"] = sep.join(path)


def add_rc_entry(fn_rc, err_code, err_msg, args=[]):
    """
    Adds a new return code entry to the return codes array

    The syntax for this function is:
      - without arguments for the format string:
          add_rc_entry(fn_rc, exception_number, error_format_string)
      - with a single argument for the format string:
          add_rc_entry(fn_rc, exception_number, error_format_string, [ [ key_string, value_string ] ])
      - with multiple arguments for the format string:
          add_rc_entry(fn_rc, exception_number, error_format_string,
              [
                  [ key_string, value_string ],
                  [ key_string, value_string ],
                  ...
              ]
          )

    Used by the drbdmanage server
    """
    try:
        if (type(err_code) is not int or
            type(err_msg) is not str or
            type(args) is not list):
            # One or multiple arguments of incorrect type
            raise TypeError("add_rc_entry(): incorrect input types")

        s_args = []
        for item in args:
            if type(item) is not list:
                raise TypeError
            # Will raise ValueError if there are not exactly two elements in the list
            key, value = item
            if not isinstance(key, str):
                raise TypeError("key should be str, is %s" % type(key))
            if not isinstance(value, (str, int, float, dbus.String)):
                raise TypeError("value should be str, is %s" % type(value))

            s_args.append([str(key), str(value)])

        rc_entry = [ err_code, err_msg, s_args ]
        fn_rc.append(rc_entry)
    except (TypeError, ValueError) as e:
        logging.error("Implementation error: Incorrect use of drbdmanage.utils.add_rc_entry(): %s" % e)


def serial_filter(serial, objects):
    """
    Generator for iterating over objects with obj_serial > serial
    """
    for obj in objects:
        obj_serial = obj.get_props().get_prop(consts.SERIAL)
        if obj_serial is None or obj_serial > serial:
            yield obj


def props_filter(source, filter_props):
    """
    Generator for iterating over objects that match filter properties
    """
    for drbdobj in source:
        if drbdobj.filter_match(filter_props):
            yield drbdobj


def get_terminal_size():
    def ioctl_GWINSZ(term_fd):
        term_dim = None
        try:
            import fcntl
            import termios
            import struct
            term_dim = struct.unpack(
                'hh',
                fcntl.ioctl(term_fd, termios.TIOCGWINSZ, '1234')
            )
        except:
            pass
        return term_dim
    # Assign the first value that's not a NoneType
    term_dim = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not term_dim:
        try:
            term_fd = os.open(os.ctermid(), os.O_RDONLY)
            term_dim = ioctl_GWINSZ(term_fd)
            os.close(term_fd)
        except:
            pass
    try:
        (term_width, term_height) = int(term_dim[1]), int(term_dim[0])
    except:
        term_width  = DEFAULT_TERM_WIDTH
        term_height = DEFAULT_TERM_HEIGHT
    return term_width, term_height


def generate_secret():
    """
    Generates a random value for a DRBD resource's shared secret

    @return: Base64-encoded random binary data
    @rtype:  str
    """
    secret = None
    try:
        secret = str(base64.b64encode(os.urandom(SECRET_LEN)))
    except:
        # Fallback to uuid if the usual
        # method of secret generation fails
        secret = uuid.uuid4()

    return secret


def generate_gi_hex_string():
    """
    Generates a DRBD generation identifier

    @return: hex string representation of a DRBD generation identifier
    """
    gi_data = bytearray(os.urandom(8))
    hex_str = ""
    for value in gi_data:
        hex_str += "%X" % (value)
    return hex_str


def map_val_or_dflt(map_obj, key, dflt):
    """
    Returns a map value if its key exists, otherwise return a default value
    """
    try:
        return map_obj[key]
    except KeyError:
        return dflt


def bool_to_string(flag):
    """
    Return identifiers from module drbdmanage.consts for boolean values
    """
    return consts.BOOL_TRUE if flag else consts.BOOL_FALSE


def string_to_bool(text):
    """
    Return boolean values for identifiers from module drbdmanage.consts
    """
    if text is not None:
        if text == consts.BOOL_TRUE:
            return True
        elif text == consts.BOOL_FALSE:
            return False
    raise ValueError


def read_lines(in_file):
    """
    Generator that yields the content of a file line-by-line

    This generator enables you to replace a loop like this:
    while True:
        line = in_file.readline()
        if not len(line) > 0:
            break
        <... code ...>
    with the simpler variant:
    for line in read_lines(in_file):
        <... code ...>
    """
    lines_available = True
    while lines_available:
        line = in_file.readline()
        if len(line) > 0:
            yield line
        else:
            lines_available = False


def is_any_set(state, flags):
    """
    Returns True if any of the flags are set in the state value, else False
    """
    return (True if (state & flags) != 0 else False)


def is_any_unset(state, flags):
    """
    Returns True if any of the flags are NOT set in the state value, else False
    """
    return (True if (state & flags) != flags else False)


def is_all_set(state, flags):
    """
    Returns True if all of the flags are set in the state value, else False
    """
    return (True if (state & flags) == flags else False)


def is_all_unset(state, flags):
    """
    Returns True if all of the flags are unset in the state value, else False
    """
    return (True if (state & flags) == 0 else False)


def filter_prohibited(to_filter, prohibited):
    for k in prohibited:
        if k in to_filter:
            del(to_filter[k])
    return to_filter


def filter_allowed(to_filter, allowed):
    for k in to_filter.keys():
        if k not in allowed:
            del(to_filter[k])
    return to_filter

# Function name aliases
is_set   = is_all_set
is_unset = is_all_unset


class DataHash(object):

    """
    Encapsulates drbdmanage's hashing algorithm; (SHA-256, currently)
    """

    HASH_LEN  = 32 # SHA-256
    _hashalgo = None
    _hash     = None # as hex-string


    def __init__(self):
        self._hashalgo = hashlib.new("sha256")


    def update(self, data):
        """
        Updates the hash by running the hash algorithm on the supplied data
        """
        self._hashalgo.update(data)


    def get_hex_hash(self):
        """
        Finishes hashing and returns the hash value in hexadecimal format

        This function returns a hexadecimal representation of the hash value

        @return: hash value in hexadecimal format
        @rtype:  str
        """
        if self._hash is None:
            self._hash = self._hashalgo.hexdigest()
        return self._hash


    def get_hex_hash_len(self):
        """
        Returns the length of the hex-format hash value in bytes

        @return: number of bytes required to store the hexadecimal
                 representation of the hash value
        """
        return self.HASH_LEN * 2


class NioLineReader(object):

    """
    Nonblocking I/O implementation for 'drbdsetup events' tracing
    """

    READBUFSZ   =  512

    _file     = None
    _data     = None
    _lines    = None

    def __init__(self, in_file):
        self._file     = in_file
        self._text     = ""
        self._lines    = []


    def get_file(self):
        """
        Returns the file/stream this NioReader instance is connected to
        """
        return self._file


    def readline(self):
        """
        Return the next line of text if one is available and buffer any
        more lines of text until they are requested, or, if no full
        line of text is available, return None and buffer text until a full
        line of text is available.

        WARNING:
        Trying to optimize this method will very likely break something.
        This is used for nonblocking I/O, and many other functions like
        readinto(bytearray), etc. failed surprisingly in all imaginable ways.
        Some seem to work at first, but fail in some special cases.
        """
        line = None
        if len(self._lines) > 0:
            line = self._lines.pop(0)
        else:
            while line is None:
                try:
                    # may the force be with you:
                    data = self._file.read(self.READBUFSZ)
                except IOError:
                    # Resource temporarily unavailable (errno 11)
                    # check for len(data) == 0 below skipped, as it does
                    # not help either
                    break
                if data is None:
                    # no more data available for reading
                    break
                if len(data) == 0:
                    # this case does not seem to happen,
                    # but just to be sure...
                    break
                else:
                    self._text += data
                lastidx = 0
                while True:
                    idx = self._text.find("\n", lastidx)
                    if idx != -1:
                        # include newline character
                        idx += 1
                        if line is None:
                            line = self._text[lastidx:idx]
                        else:
                            self._lines.append(self._text[lastidx:idx])
                        lastidx = idx
                    else:
                        break
                if lastidx != 0:
                    self._text = self._text[lastidx:]
        return line


class ExternalCommand(object):

    _args    = None

    _trace_exec_args = None
    _trace_exit_code = None
    source           = None
    trace_id         = None

    def __init__(self, source, command_args, trace_id=None, trace_exec_args=None, trace_exit_code=None):
        self._args = command_args
        self._trace_exec_args = trace_exec_args
        self._trace_exit_code = trace_exit_code
        self.source           = source
        self.trace_id         = trace_id

    def run(self):
        epoll = select.epoll()

        # Log the command arguments
        if self._trace_exec_args is not None:
            self._trace_exec_args(self.source, self.trace_id, self._args)

        # Spawn the process and pipe stdout/stderr
        proc = subprocess.Popen(
            self._args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            close_fds=True
        )

        # Get the stdout/stderr file descriptors
        out_fd = proc.stdout.fileno()
        err_fd = proc.stderr.fileno()

        # Register the file descriptors for polling
        poll_mask = select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP
        epoll.register(out_fd, poll_mask)
        epoll.register(err_fd, poll_mask)

        # Make the file descriptors non-blocking
        fcntl.fcntl(out_fd, fcntl.F_SETFL, fcntl.F_GETFL | os.O_NONBLOCK)
        fcntl.fcntl(err_fd, fcntl.F_SETFL, fcntl.F_GETFL | os.O_NONBLOCK)

        # Set up NioLineReader instances
        out_reader = NioLineReader(proc.stdout)
        err_reader = NioLineReader(proc.stderr)

        # Read piped stdout/stderr whenever they become ready
        out_ok = True
        err_ok = True
        while out_ok or err_ok:
            try:
                events = epoll.poll()
                for (poll_fd, event_id) in events:
                    if event_id == select.EPOLLIN:
                        if poll_fd == err_fd:
                            self._read_stream(err_reader, self.stderr_handler)
                        elif poll_fd == out_fd:
                            self._read_stream(out_reader, self.stdout_handler)
                    else:
                        epoll.unregister(poll_fd)
                        if poll_fd == err_fd:
                            err_ok = False
                        elif poll_fd == out_fd:
                            out_ok = False
            except IOError as io_err:
                # Retry interrupted system calls
                if not io_err.errno == errno.EINTR:
                    # End I/O for anything other than an interrupted system call
                    break

        # Close the pipes
        proc.stdout.close()
        proc.stderr.close()

        # Wait for the external process to exit
        exit_code = proc.wait()

        # Log the command's exit code
        if self._trace_exit_code is not None:
            self._trace_exit_code(self.source, self.trace_id, self._args[0], exit_code)

        return exit_code

    def _read_stream(self, reader, handler):
        while True:
            line = reader.readline()
            if line is None:
                break
            handler(self.source, self._args[0], self.trace_id, line)

    def stdout_handler(self, source, command, trace_id, line):
        pass

    def stderr_handler(self, source, command, trace_id, line):
        pass


class ExternalCommandBuffer(ExternalCommand):

    _out_buffer = None
    _err_buffer = None
    _command    = None

    def __init__(self, source, command_args, trace_id=None, trace_exec_args=None, trace_exit_code=None):
        super(ExternalCommandBuffer, self).__init__(
            source, command_args, trace_id, trace_exec_args, trace_exit_code
        )
        self._out_buffer = []
        self._err_buffer = []
        self._command  = command_args[0]

    def stdout_handler(self, source, command, trace_id, line):
        self._out_buffer.append(line)

    def stderr_handler(self, source, command, trace_id, line):
        self._err_buffer.append(line)

    def log_stdout(self, log_handler=logging.error):
        if self.trace_id is not None:
            for line in self._out_buffer:
                log_handler("[trace_id %s] %s/stdout: %s"
                            % (self.trace_id, self._command, line))
        else:
            for line in self._out_buffer:
                log_handler("%s/stdout: %s"
                            % (self._command, line))

    def log_stderr(self, log_handler=logging.error):
        if self.trace_id is not None:
            for line in self._err_buffer:
                log_handler("[trace_id %s] %s/stderr: %s"
                            % (self.trace_id, self._command, line))
        else:
            for line in self._err_buffer:
                log_handler("%s/stderr: %s"
                            % (self._command, line))

    def get_stdout(self):
        return self._out_buffer

    def get_stderr(self):
        return self._err_buffer

    def clear(self):
        self._out_buffer = []
        self._err_buffer = []


class ExternalCommandLogger(ExternalCommand):

    _log_handler = None

    def __init__(self, source, command_args,
        trace_id=None, trace_exec_args=None, trace_exit_code=None,
        log_handler=logging.debug
    ):
        super(ExternalCommandLogger, self).__init__(
            source, command_args, trace_id, trace_exec_args, trace_exit_code
        )
        self._log_handler = log_handler

    def stdout_handler(self, source, command, trace_id, line):
        self._log_handler("%s: [trace_id %s] %s/stdout: %s"
                    % (source, trace_id, command, line))

    def stderr_handler(self, source, command, trace_id, line):
        self._log_handler("%s: [trace_id %s] %s/stderr: %s"
                    % (source, trace_id, command, line))


class SizeCalc(object):

    """
    Methods for converting decimal and binary sizes of different magnitudes
    """

    _base_2  = 0x0200
    _base_10 = 0x0A00

    UNIT_B   =  0 | _base_2
    UNIT_kiB = 10 | _base_2
    UNIT_MiB = 20 | _base_2
    UNIT_GiB = 30 | _base_2
    UNIT_TiB = 40 | _base_2
    UNIT_PiB = 50 | _base_2
    UNIT_EiB = 60 | _base_2
    UNIT_ZiB = 70 | _base_2
    UNIT_YiB = 80 | _base_2

    UNIT_kB =   3 | _base_10
    UNIT_MB =   6 | _base_10
    UNIT_GB =   9 | _base_10
    UNIT_TB =  12 | _base_10
    UNIT_PB =  15 | _base_10
    UNIT_EB =  18 | _base_10
    UNIT_ZB =  21 | _base_10
    UNIT_YB =  24 | _base_10


    @classmethod
    def convert(cls, size, unit_in, unit_out):
        """
        Convert a size value into a different scale unit

        Convert a size value specified in the scale unit of unit_in to
        a size value given in the scale unit of unit_out
        (e.g. convert from decimal megabytes to binary gigabytes, ...)

        @param   size: numeric size value
        @param   unit_in: scale unit selector of the size parameter
        @param   unit_out: scale unit selector of the return value
        @return: size value converted to the scale unit of unit_out
                 truncated to an integer value
        """
        fac_in   = ((unit_in & 0xffffff00) >> 8) ** (unit_in & 0xff)
        div_out  = ((unit_out & 0xffffff00) >> 8) ** (unit_out & 0xff)
        return (size * fac_in // div_out)


    @classmethod
    def convert_round_up(cls, size, unit_in, unit_out):
        """
        Convert a size value into a different scale unit and round up

        Convert a size value specified in the scale unit of unit_in to
        a size value given in the scale unit of unit_out
        (e.g. convert from decimal megabytes to binary gigabytes, ...).
        The result is rounded up so that the returned value always specifies
        a size that is large enough to contain the size supplied to this
        function.
        (e.g., for 100 decimal Megabytes (MB), which equals 100 million bytes,
         returns 97,657 binary kilobytes (kiB), which equals 100 million
         plus 768 bytes and therefore is large enough to contain 100 megabytes)

        @param   size: numeric size value
        @param   unit_in: scale unit selector of the size parameter
        @param   unit_out: scale unit selector of the return value
        @return: size value converted to the scale unit of unit_out
        """
        fac_in   = ((unit_in & 0xffffff00) >> 8) ** (unit_in & 0xff)
        div_out  = ((unit_out & 0xffffff00) >> 8) ** (unit_out & 0xff)
        byte_sz  = size * fac_in
        if byte_sz % div_out != 0:
            result = (byte_sz / div_out) + 1
        else:
            result = byte_sz / div_out
        return result


class Selector(object):

    """
    Helper class for selection lists
    """

    _keys = None


    def __init__(self, keys_list):
        self._keys = {}
        if keys_list is not None:
            for key in keys_list:
                self._keys[key] = None


    def all_selector(self, key):
        """
        Returns true for any key
        """
        return True


    def list_selector(self, key):
        """
        Returns true for keys that appear in the Selector's list of keys
        """
        return (True if key is not None and key in self._keys else False)


def _aux_prop_name(key):
    """
    Returns the key of auxiliary properties without prefix, otherwise None

    If 'key' starts with the prefix for auxiliary properties, the name without
    the prefix is returned;
    otherwise, None is returned
    """
    if str(key).startswith(consts.AUX_PROP_PREFIX):
        return key[len(consts.AUX_PROP_PREFIX):]
    else:
        return None


def _is_aux_prop_name(key):
    """
    Returns True if the key names an auxiliary property, otherwise False

    If 'key' starts with the prefix for auxiliary properties, True is returned,
    otherwise, False is returned.
    """
    if str(key).startswith(consts.AUX_PROP_PREFIX):
        return True
    return False


def dict_to_aux_props(props):
    """
    Turns a dictionary in to an auxiliary properties dictionary

    Generates an auxiliary properties dictionary from an existing
    dictionary by prefixing each key name with the auxiliary
    property prefix
    """
    aux_props = {}
    for (key, val) in props.iteritems():
        aux_key = consts.AUX_PROP_PREFIX + str(key)
        aux_props[aux_key] = str(val)
    return aux_props


def aux_props_to_dict(props):
    """
    Extracts auxiliary properties from a dictionary of properties

    The auxiliary property prefix is removed from the key names
    in the dictionary that is returned
    """
    aux_props = {}
    for (key, val) in props.iteritems():
        aux_key = _aux_prop_name(key)
        if aux_key is not None:
            aux_props[aux_key] = val
    return aux_props


def split_main_aux_props(props):
    """
    Splits a dictionary into two (main, aux) dictionaries

    The first dictionary will contain drbdmanage-defined properties
    (main properties). The second dictionary will contain user-added
    properties (auxiliary properties).
    """
    main_props = {}
    aux_props  = {}
    for (key, val) in props.iteritems():
        aux_key = _aux_prop_name(key)
        if aux_key is not None:
            aux_props[aux_key] = val
        else:
            main_props[key]    = val
    return main_props, aux_props


def merge_aux_props(obj, props):
    """
    Merges all auxiliary properties contained in props into obj's properties

    All auxiliary properties contained in props are extracted and merged into
    the target object's (obj) properties (props). Existing auxiliary properties
    in the target object are updated with the new values specified in props.
    """
    aux_props = {}
    for (key, val) in props.iteritems():
        if _is_aux_prop_name(key):
            aux_props[key] = val
    obj.get_props().merge_props(aux_props)


def aux_props_selector(props):
    """
    Selects auxiliary properties from a dictionary of properties
    """
    for (key, val) in props.iteritems():
        if _is_aux_prop_name(key):
            yield (key, val)


def key_value_string(key, value):
    """
    Creates a "key=value" string from a key and a value
    """
    return str(key) + "=" + str(value)


def align_up(value, alignment):
    value     = long(value)
    alignment = long(alignment)
    if value % alignment != 0:
        value = ((value / alignment) + 1) * alignment
    return value


def align_down(value, alignment):
    value     = long(value)
    alignment = long(alignment)
    return (value / alignment) * alignment


def ceiling_divide(dividend, divisor):
        dividend = long(dividend)
        divisor  = long(divisor)
        quotient = dividend / divisor
        if dividend % divisor != 0:
            quotient += 1
        return quotient


def approximate_size_string(size_kiB):
    """
    Produce human readable size information as a string
    """
    units = []
    units.append("kiB")
    units.append("MiB")
    units.append("GiB")
    units.append("TiB")
    units.append("PiB")
    max_index = len(units)

    index = 0
    counter = 1
    magnitude = 1 << 10
    while counter < max_index:
        if size_kiB >= magnitude:
            index = counter
        else:
            break
        magnitude = magnitude << 10
        counter += 1
    magnitude = magnitude >> 10

    size_str = None
    if size_kiB % magnitude != 0:
        size_unit = float(size_kiB) / magnitude
        size_loc = locale.format('%3.2f', size_unit, grouping=True)
        size_str = "%s %s" % (size_loc, units[index])
    else:
        size_unit = size_kiB / magnitude
        size_str = "%d %s" % (size_unit, units[index])

    return size_str


def debug_log_exec_args(source, exec_args):
    """
    Logs the command line of external commands at the DEBUG level
    """
    logging.debug(
        "%s: Running external command: %s"
        % (source, " ".join(exec_args))
    )

def info_log_exec_args(source, exec_args):
    """
    Logs the command line of external commands at the INFO level
    """
    logging.info(
        "%s: Running external command: %s"
        % (source, " ".join(exec_args))
    )

def debug_trace_exec_args(source, trace_id, args):
    """
    Logs the trace id and command line of external commands at the DEBUG level
    """
    generic_trace_exec_args(logging.debug, source, trace_id, args)

def info_trace_exec_args(source, trace_id, args):
    """
    Logs the trace id and command line of external commands at the DEBUG level
    """
    generic_trace_exec_args(logging.info, source, trace_id, args)

def generic_trace_exec_args(log_handler, source, trace_id, args):
    """
    Logs the trace id and command line of external commands using the specified handler function
    """
    if trace_id is not None:
        log_handler(
            "%s: [trace_id %s] Running external command: %s"
            % (source, trace_id, " ".join(args))
        )
    else:
        log_handler(
            "%s: Running external command: %s"
            % (source, " ".join(args))
        )

def debug_trace_exit_code(source, trace_id, command, exit_code):
    """
    Logs the trace id and exit code of external commands at the DEBUG level
    """
    generic_trace_exit_code(logging.debug, source, trace_id, command, exit_code)

def info_trace_exit_code(source, trace_id, command, exit_code):
    """
    Logs the trace id and exit code of external commands at the INFO level
    """
    generic_trace_exit_code(logging.info, source, trace_id, command, exit_code)

def smart_trace_exit_code(source, trace_id, command, exit_code):
    """
    Logs the trace id and exit code of external commands

    Logs at the INFO level for exit code 0 and at the
    ERROR level for any other exit code
    """
    if exit_code == 0:
        generic_trace_exit_code(logging.info, source, trace_id, command, exit_code)
    else:
        generic_trace_exit_code(logging.error, source, trace_id, command, exit_code)

def generic_trace_exit_code(log_handler, source, trace_id, command, exit_code):
    """
    Logs the trace id and exit code of external commands using the specified handler function
    """
    if trace_id is not None:
        log_handler(
            "%s: [trace_id %s] External command '%s': Exit code %d"
            % (source, trace_id, command, exit_code)
        )
    else:
        log_handler(
            "%s: External command '%s': Exit code %d"
            % (source, command, exit_code)
        )


def pickle_dbus(name, args, kwargs):
    def pickle_dbus_int(x):
        return int, (int(x),)

    def pickle_dbus_str(x):
        return str, (str(x),)

    def pickle_dbus_boolean(x):
        return bool, (bool(x),)

    def pickle_dbus_double(x):
        return float, (float(x),)

    def pickle_dbus_array(x):
        return list, (list(x),)

    def pickle_dbus_dictionary(x):
        return dict, (dict(x),)

    copy_reg.pickle(dbus.Int16, pickle_dbus_int)
    copy_reg.pickle(dbus.Int32, pickle_dbus_int)
    copy_reg.pickle(dbus.Int64, pickle_dbus_int)
    copy_reg.pickle(dbus.String, pickle_dbus_str)
    copy_reg.pickle(dbus.Boolean, pickle_dbus_boolean)
    copy_reg.pickle(dbus.Double, pickle_dbus_double)
    copy_reg.pickle(dbus.Array, pickle_dbus_array)
    copy_reg.pickle(dbus.Dictionary, pickle_dbus_dictionary)

    return pickle.dumps({'name': name, 'args': args, 'kwargs': kwargs})


def log_in_out(f):
    @wraps(f)
    def wrapper(*args, **kwds):
            class_str = args[0].__class__.__name__
            method_str = f.__name__
            logging.getLogger("")
            logging.debug('Entering %s.%s' % (class_str, method_str))
            ret = f(*args, **kwds)
            logging.debug('Exited %s.%s' % (class_str, method_str))
            return ret
    return wrapper

def parse_event_line(event_line):
    obj_type = None
    obj_props = {}
    event_tokens = event_line.split(' ')
    try:
        # Check whether there are tokens
        if len(event_tokens) >= 1:
            mode = event_tokens.pop(0)
            # Check whether the first token is an empty string or a new-line character,
            # because str.split() produces tokens for empty strings
            if mode != "" and mode != "\n":
                # If the token is something else, check whether it equals "exists"
                if mode != "exists":
                    raise EventException
                # If this is an "exists" line, then there needs to be at least one more token
                obj_type = event_tokens.pop(0)
                # Iterate over the following key-value pairs
                for kv_pair in event_tokens:
                    kv_split = kv_pair.find(":")
                    # Ignore anything that is not a key-value pair
                    if kv_split != -1:
                        key = kv_pair[:kv_split]
                        value = kv_pair[kv_split + 1:]
                        obj_props[key] = value
    except IndexError:
        raise EventException
    return obj_type, obj_props
