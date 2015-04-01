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
Generalized utility functions and classes for drbdmanage
"""

import sys
import os
import hashlib
import base64
import drbdmanage.consts as consts

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

# Source for random data
RANDOM_SOURCE = "/dev/urandom"

# Length of random data for generating shared secrets
SECRET_LEN    = 15

# Default terminal dimensions
# Used by get_terminal_size()
DEFAULT_TERM_WIDTH  = 80
DEFAULT_TERM_HEIGHT = 25


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


def plugin_import(path):
    """
    Imports a plugin

    @param   path: Python path specification
    @return: new instance of the plugin; None on error
    """
    p_mod   = None
    p_class = None
    p_inst  = None
    try:
        if path is not None:
            idx = path.rfind(".")
            if idx != -1:
                p_name = path[idx + 1:]
                p_path = path[:idx]
            else:
                p_name = path
                p_path = ""
            p_mod   = __import__(p_path, globals(), locals(), [p_name], -1)
            p_class = getattr(p_mod, p_name)
            p_inst  = p_class()
    except Exception:
        pass
    return p_inst


def extend_path(ext_path):
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
    path = ""
    sep  = ":"
    try:
        path = os.environ["PATH"]
    except KeyError:
        pass
    path_items = path.split(sep)
    ext_path_items = ext_path.split(sep)
    for item in ext_path_items:
        if item not in path_items:
            path_items.append(item)
    path = sep.join(path_items)
    # this will implicitly update the actual PATH environment variable
    # by means of a call to putenv() according to python documentation
    os.environ["PATH"] = path


def add_rc_entry(fn_rc, err_code, err_msg, *args):
    """
    Add a new return code entry to the return codes array

    Used by the drbdmanage server
    """
    if type(args) is dict:
        # Note: this seemingly superfluous intermediate step is basically
        #       something like a type cast that fixes a PyLint warning
        args_dict = dict(args)
        args = [(key, val) for key, val in args_dict.iteritems()]
    rc_entry = [ err_code, err_msg, args ]
    fn_rc.append(rc_entry)


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
            import fcntl, termios, struct
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
    f_rnd  = None
    try:
        f_rnd = open(RANDOM_SOURCE, "r")
        rnd   = bytearray(SECRET_LEN)
        count = f_rnd.readinto(rnd)
        # seems useless, but this is required for base64.b64encode() to
        # work on python 2.6; otherwise, it crashes with a type error
        s_rnd = str(rnd)
        if count == SECRET_LEN:
            secret = str(base64.b64encode(s_rnd))
        f_rnd.close()
    except IOError:
        if f_rnd is not None:
            try:
                f_rnd.close()
            except IOError:
                pass
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


class MetaData(object):

    # Size of the static-size DRBD meta data header
    MD_HEADER_SIZE = 4096

    # Size of the activity log area
    # TODO: this should be variable in future versions to account for
    #       different al-stripe-size settings, etc.
    AL_SIZE = 32768

    # Bitmap coverage for the meta data header (MD_HEADER_SIZE + AL_SIZE)
    # Sized for the theoretical limit of 63 peers (64 node cluster)
    BM_HEADER = 126

    # Size of data area covered by one bitmap byte
    BM_BYTE_COVERAGE = 32768

    # Unit of alignment; must be a multiple of 1024
    ALIGNMENT = 4096


    @classmethod
    def get_gross_data_kiB(self, net_data_kiB, peers):
        # Do not allow volumes with a negative or empty size
        if net_data_kiB < MetaData.ALIGNMENT / 1024:
            net_data_kiB = MetaData.ALIGNMENT

        # Round up to the next alignment boundary
        net_data_b = long(net_data_kiB * 1024)
        if net_data_b % MetaData.ALIGNMENT != 0:
            net_data_b = ((long(net_data_b / MetaData.ALIGNMENT) + 1) *
                          MetaData.ALIGNMENT)

        bitmap_size_b = ((long(net_data_b / MetaData.BM_BYTE_COVERAGE) + 1) *
                         peers + (MetaData.ALIGNMENT - 1) + MetaData.BM_HEADER)

        # calculate additional bitmap size required so the bitmap
        # can cover the space it occupies itself
        # (which increases the bitmap size, therefore requiring some more
        #  bitmap space to cover the additional space, and thereby increases
        #  the space again, so this is obviously a recursive problem, and
        #  that's where the unintuitive calculation comes from)
        # This is an upper-bound calculation, so the gross data size
        # calculated will be a sligth overestimate
        # (It overestimates the bitmap's self-coverage by the size of a bitmap
        #  for up to one additional gigabyte of net data)
        bitmap_overhead_units = long(net_data_b / (2 ** 30))
        if net_data_b % (2 ** 30) != 0:
            bitmap_overhead_units += 1
        bitmap_overhead = bitmap_overhead_units * (peers ** 2) + peers + 1

        # DRBD meta data headers (activity log & static size header)
        # TODO: The activity log size is actually variable
        headers = MetaData.MD_HEADER_SIZE + MetaData.AL_SIZE

        gross_data_b = net_data_b + bitmap_size_b + bitmap_overhead + headers

        # Align to the upper alignment boundary
        gross_data_blocks = gross_data_b / MetaData.ALIGNMENT
        if gross_data_b % MetaData.ALIGNMENT != 0:
            gross_data_blocks += 1

        gross_data_kiB = gross_data_blocks * (MetaData.ALIGNMENT / 1024)

        return gross_data_kiB


    @classmethod
    def get_net_data_kiB(self, gross_data_kiB, peers):
        # Align negative sizes to zero:
        if gross_data_kiB < 0:
            gross_data_kiB = 0

        # Round down to the next alignment boundary
        gross_data_b = (long(gross_data_kiB / (MetaData.ALIGNMENT / 1024)) *
                        MetaData.ALIGNMENT)

        # calculate the size of the area occupied by the bitmap so that
        # the calculation will overerstimate the size of the bitmap area
        # in the same way that get_gross_data_kiB() does
        bitmap_size_b = ((long(gross_data_b / MetaData.BM_BYTE_COVERAGE) + 1) *
                         peers + (MetaData.ALIGNMENT - 1) + MetaData.BM_HEADER)
        # add-in the get_gross_data_kiB() function's overerstimate for the
        # bitmap's self-coverage
        # (The get_gross_data_kiB() function overestimates the bitmap's
        #  self-coverage by the size of a bitmap for up to one additional
        #  gigabyte of net data)
        bitmap_overhead_b = (peers ** 2) + peers + 1

        # DRBD meta data headers (activity log & static size header)
        # TODO: The activity log size is actually variable
        headers = MetaData.MD_HEADER_SIZE + MetaData.AL_SIZE

        overhead = bitmap_size_b + bitmap_overhead_b + headers
        net_data_b = 0
        if overhead < gross_data_b:
            net_data_b = gross_data_b - overhead

        # Align/truncate to the lower alignment boundary
        net_data_blocks = net_data_b / MetaData.ALIGNMENT
        net_data_kiB    = net_data_blocks * (MetaData.ALIGNMENT / 1024)

        return net_data_kiB


    @classmethod
    def align(cls, size):
        result = size
        if size % MetaData.ALIGNMENT != 0:
            result = ((int(size / MetaData.ALIGNMENT) + 1) *
                      MetaData.ALIGNMENT)
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
