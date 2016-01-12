#!/usr/bin/env python2
# -*- coding: utf-8 -*-
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
Generalized utility functions and classes for drbdmanage
"""

import dbus
import os
import sys
import hashlib
import base64
import operator
import subprocess
import uuid
import drbdmanage.consts as consts
import logging
import ConfigParser
from drbdmanage.exceptions import SyntaxException
from drbdmanage.consts import (SERVER_CONFFILE, PLUGIN_PREFIX, KEY_DRBD_CONFPATH,
                               KEY_DRBDCTRL_VG, KEY_SAT_CFG_ROLE, KEY_COLORS, KEY_UTF8)

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
    def __init__(self, colors=True, utf8=False):
        self.r_just = False
        self.got_column = False
        self.got_row = False
        self.groups = []
        self.header = []
        self.table = []
        self.coloroverride = []
        self.view = None
        self.showseps = False
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


def ssh_exec(cmdname, ip, name, cmdline, quiet=False):
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
            subprocess.check_call(ssh_cmd)
            return True
    except subprocess.CalledProcessError:
        sys.stderr.write("Error: Attempt to execute the %s command remotely"
                         "failed\n" % (cmdname))

    return False


def checkrange(v, i, j):
    return i <= v <= j


def rangecheck(i, j):
    def range(v):
        import argparse.argparse as argparse
        v = int(v)
        if not checkrange(v, i, j):
            raise argparse.ArgumentTypeError('Range: [%d, %d]' % (i, j))
        return v
    return range


def get_uname():
    node_name = None
    try:
        uname = os.uname()
        if len(uname) >= 2:
            node_name = uname[1]
    except OSError:
        pass
    return node_name


def load_server_conf_file(localonly=False):
    """
    Try to load the server configuration.
    """
    import errno

    cfgdict = {'local': {}, 'plugins': {}}

    try:
        cfg = ConfigParser.RawConfigParser()
        cfg.read(SERVER_CONFFILE)
        for section in cfg.sections():
            if section.startswith('LOCAL'):
                in_file_cfg = dict(cfg.items(section))
                if not cfg.has_option(section, 'force'):
                    final_config = filter_allowed(in_file_cfg.copy(), (KEY_DRBDCTRL_VG,
                                                                       'extend-path',
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
    except IOError as ioerr:
        if ioerr.errno == errno.EACCES:
            logging.warning(
                "cannot open configuration file '%s', permission denied"
                % (SERVER_CONFFILE)
            )
        elif ioerr.errno != errno.ENOENT:
            logging.warning(
                "cannot open configuration file '%s', "
                "error returned by the OS is: %s"
                % (SERVER_CONFFILE, ioerr.strerror)
            )

    if localonly:
        return cfgdict['local']
    else:
        return cfgdict


# mainly used for DrbdSetupOpts()
# but also usefull for 'handlers' subcommand
def filter_new_args(unsetprefix, args):
    new = dict()
    for k, v in args.__dict__.iteritems():
        if v is not None and k != "func" and k != "optsobj" and k != "common" and k != "command":
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
                             ' and option at the same time!\n')
            return False
    return new


class DrbdSetupOpts():
    def __init__(self, command):
        import sys
        import xml.etree.ElementTree as ET
        self.command = command
        self.config = {}
        self.unsetprefix = 'unset'

        out = False
        for cmd in ('drbdsetup', '/sbin/drbdsetup'):
            try:
                out = check_output([cmd, "xml-help", self.command])
                break
            except OSError:
                pass
        if not out:
            sys.stderr.write("Could not execute drbdsetup\n")
            sys.exit(1)

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

    def genArgParseSubcommand(self, subp):
        sp = subp.add_parser(self.command, description=self.config['help'])

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


def get_free_number(min_nr, max_nr, nr_list):
    """
    Returns the first number in the range min..max that is not in nr_list

    In the range min to max, finds and returns a number that is not in the
    supplied list of numbers.
    min and max must be positive integers, and nr_list must be a list of
    positive integers in the range from min to max

    @param   min_nr:  range start, positive number, less than or equal to max
    @param   max_nr:  range end, positive number, greater than or equal to min
    @param   nr_list: list of numbers within the range min..max
    @type    nr_list: list of int or long values
    @return: first free number within min..max; or -1 on error
    """
    free_nr = -1
    if min_nr >= 0 and min_nr <= max_nr:
        items = len(nr_list)
        if items == 0:
            free_nr = min_nr
        else:
            nr_list.sort()
            idx = 0
            last_nr = min_nr - 1
            while free_nr == -1 and idx < items:
                current_nr = nr_list[idx]
                if current_nr - last_nr > 1:
                    free_nr = last_nr + 1
                else:
                    idx += 1
                    last_nr = current_nr
            if free_nr == -1 and last_nr < max_nr:
                free_nr = last_nr + 1
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
    full_path = filename
    if prefix is not None:
        if len(prefix) > 0 and (not prefix.endswith("/")):
            prefix += "/"
        full_path = prefix + filename
    return full_path


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
            if not isinstance(value, (str, int, dbus.String)):
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


def debug_log_exec_args(source, exec_args):
    """
    Logs the arguments used to execute an external command
    """
    logging.debug(
        "%s: Running external command: %s"
        % (source, " ".join(exec_args))
    )
