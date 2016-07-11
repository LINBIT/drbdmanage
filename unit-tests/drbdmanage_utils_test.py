#!/usr/bin/env python2
"""
  drbdmanage - management of distributed DRBD9 resources
  Copyright (C) 2016   LINBIT HA-Solutions GmbH
                             Author: Hayley Swimelar

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
import string
import sys
import unittest

from StringIO import StringIO

import drbdmanage.consts as const
import drbdmanage.exceptions as DME
import drbdmanage.utils as utils

from drbdmanage.argparse import argparse

# Python 3 compatibility
try:
    import unittest.mock as mock
except ImportError:
    try:
        import mock
    except ImportError as err:
        raise err("module 'mock' is required to run these tests")


class TableTests(unittest.TestCase):

    def setUp(self):
        self.table = utils.Table()

    def tearDown(self):
        self.table = None

    def test_add_column(self):
        """appends columns to table.header"""
        self.table.add_column("test row")
        self.table.add_column("color row", color=True)

        self.assertDictContainsSubset(
            {"name": "test row"}, self.table.header[0]
        )
        self.assertDictContainsSubset(
            {"name": "color row", "color": True}, self.table.header[1]
        )

    def test_add_column_no_collumns_after_rows(self):
        """raises SyntaxException if a row has already been added"""
        self.table.add_column("good column")
        self.table.add_row(["test row"])
        self.assertRaises(
            DME.SyntaxException, self.table.add_column, "bogus column"
        )

    def test_add_row(self):
        """appends rows to table"""
        self.table.add_column("test")
        self.table.add_column("test")
        self.table.add_column("test")

        numbers = [1, 2, 3]
        strings = ["one", "two", "three"]
        self.table.add_row(numbers)
        self.table.add_row(strings)

        self.assertListEqual(self.table.table[0], numbers)
        self.assertListEqual(self.table.table[1], strings)

    def test_add_row_no_rows_without_columns(self):
        """prevent rows from being added before columns"""
        self.assertRaises(DME.SyntaxException, self.table.add_row, ["test"])

    def test_add_row_row_length_equal_column_length(self):
        """rows have the same number of elements as there are columns"""
        self.table.add_column("test")
        self.table.add_column("test")

        self.assertRaises(DME.SyntaxException, self.table.add_row, ["test"])
        self.assertRaises(
            DME.SyntaxException, self.table.add_row, ["P", "D", "X"]
        )

    def test_add_row_no_tuple_color_if_no_column_color(self):
        """tuples only have color if their header does as well"""

        self.table.add_column("test", color=utils.COLOR_RED)
        self.table.add_column("test")

        self.assertRaises(
            DME.SyntaxException,
            self.table.add_row, ["test", ("row", utils.COLOR_RED)]
        )
        self.table.add_row([("test", utils.COLOR_BLUE), "row"])


class TableShowTests(unittest.TestCase):

    def setUp(self):
        self.table = utils.Table()
        sys.stdout = StringIO()

    def tearDown(self):
        self.table = None
        sys.stdout = sys.__stdout__

    def test_show(self):
        """displays a table with no special settings"""
        self.table.add_column("test")
        self.table.add_column("header")

        self.table.add_row(["first", "row"])
        self.table.add_row([1, 2])

        self.table.show()

        self.assertIn("test", sys.stdout.getvalue())
        self.assertIn("header", sys.stdout.getvalue())
        self.assertIn("1", sys.stdout.getvalue())
        self.assertIn("2", sys.stdout.getvalue())

    def test_show_with_view(self):
        """only displays expected groups"""
        self.table.add_column("test")
        self.table.add_column("display")
        self.table.add_column("by")
        self.table.add_column("group")
        self.table.add_row([1, 2, 3, 4])

        self.table.set_view(["group", "display"])

        self.table.show()

        self.assertIn("group", sys.stdout.getvalue())
        self.assertIn("display", sys.stdout.getvalue())
        self.assertNotIn("1", sys.stdout.getvalue())
        self.assertNotIn("3", sys.stdout.getvalue())

    def test_show_with_groupbys(self):
        """sorts groups least to greatest"""

        self.table.add_column("test")
        self.table.add_column("group")
        self.table.add_row([3, 9])
        self.table.add_row([1, 7])
        self.table.add_row([8, 2])

        self.table.set_groupby(["group", "test"])

        self.table.show()
        self.assertRegexpMatches(
            sys.stdout.getvalue(),
            ".*2.*\n.*7.*\n.*9.*\n.*"
        )


class CheckrangeTests(unittest.TestCase):

    def test_checkrange(self):
        """returns true if v between i and j"""

        self.assertTrue(utils.checkrange(4, 1, 1000))
        self.assertTrue(utils.checkrange(-182, -1293, 0))
        self.assertTrue(utils.checkrange(0, 0, 0))
        self.assertFalse(utils.checkrange(182, 0, 0))
        self.assertFalse(utils.checkrange(.3, .31, .3001))


class RangecheckTests(unittest.TestCase):

    def test_rangecheck(self):
        """returns valid function"""

        five_to_ten = utils.rangecheck(5, 10)
        self.assertTrue(five_to_ten(7))

        neg_two_to_zero = utils.rangecheck(-2, 0)
        self.assertTrue(neg_two_to_zero(-1.000001))

    def test_rangecheck_error_on_invalid_range(self):
            """raise ArgumentTypeError when arg is out of range"""

            six_to_fifty = utils.rangecheck(6, 50)
            self.assertRaises(argparse.ArgumentTypeError, six_to_fifty, 65)


class CheckNodeNameTests(unittest.TestCase):

    def test_check_node_name(self):
        """returns name if name complies with RFC952 / RFC1123"""
        valid_names = ["test", "my.friendly.server", "7-samurai" "cat.wow"]

        for name in valid_names:
            self.assertEqual(name, utils.check_node_name(name))

    def test_check_node_name_invalid_names(self):
        """raise InvalidNameException for uncompliant hostnames"""

        invalid_names = [
            None, "", "#internet", "pistols@dawn", "a"*270,
            "cheeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeese"
        ]

        for name in invalid_names:
            self.assertRaises(
                DME.InvalidNameException, utils.check_node_name, name
            )
            if name is not None:
                print name + "...raised InvalidNameException as expected"

    def test_namecheck(self):
        """returns valid check type"""

        node_check = utils.namecheck(const.NODE_NAME)

        self.assertTrue(node_check("test.name"), "test.name")
        self.assertRaises(argparse.ArgumentTypeError, node_check, "long"*256)


class CheckNameTests(unittest.TestCase):

    def test_check_name(self):
        """returns the name if it meets the constraints"""
        good_name = "goodName"
        checked_name = utils.check_name(
            good_name, 4, 255, string.ascii_lowercase,
            string.ascii_letters
        )

        self.assertEqual(checked_name, good_name)

    def test_check_name_bad_name(self):
        """raises InvalidNameException on invalid names"""

        bad_name = "bad()name"
        self.assertRaises(
            DME.InvalidNameException, utils.check_name, bad_name, 1, 10,
            string.ascii_lowercase, string.ascii_letters
        )


class NameCheckTests(unittest.TestCase):

    def test_namecheck_res(self):
        """returns correct res check type"""

        res_check = utils.namecheck(const.RES_NAME)

        good_name = "backups"
        self.assertEqual(good_name, res_check(good_name))

    def test_namecheck_res_bad_name(self):
        """returned type raises ArgumentTypeError on bad res names"""

        res_check = utils.namecheck(const.RES_NAME)

        bad_name = "deposit$"
        self.assertRaises(argparse.ArgumentTypeError, res_check, bad_name)

    def test_namecheck_snap(self):
        """returns correct type for snap names"""

        res_check = utils.namecheck(const.SNAPS_NAME)

        good_name = "backups-snap"
        self.assertEqual(good_name, res_check(good_name))


class FilterNewArgs(unittest.TestCase):

    # Suppress error message from filter_new_args.
    # The message makes it appear that the test has an error.
    def setUp(self):
        sys.stderr = StringIO()

    def tearDown(self):
        sys.stderr = sys.__stderr__

    class FakeArgsObj(object):
        def __init__(self):
            self.test_string = "test_string"
            self.foo = "foo"
            self.unset_bar = "bar"
            self.true = "True"
            self.false = "False"
            self.common = "common"
            self.command = "command"
            self.optsobj = "ootsobj"
            self.func = "func"

    class UnsetSetArgsObj(object):
        def __init__(self):
            self.unset_bar = "bar"
            self.bar = "bar"

    def test_filter_new_args(self):
        """converts snake case to sausage case and excludes reserved keys"""
        new_keys = utils.filter_new_args(
            unsetprefix="unset_", args=self.FakeArgsObj()
        )

        print new_keys
        self.assertDictEqual(
            {
                'true': 'yes', 'unset-bar': 'bar', 'foo': 'foo',
                'false': 'no', 'test-string': 'test_string'
            },
            new_keys
        )

    def test_filter_new_args_set_unset(self):
        """returns False on simultaneous set/unset"""

        self.assertFalse(
            utils.filter_new_args(
                unsetprefix="unset_", args=self.UnsetSetArgsObj()
            )
        )


class GetFreeNumberTests(unittest.TestCase):

    def test_get_free_number(self):
        """returns first free number in range from list"""

        self.assertEqual(
            6, utils.get_free_number(
                5, 100, [3, 1, 1111, 5, 5, 4, 8, 10, 9]
            )
        )

    def test_get_free_number_no_free(self):
        """returns -1 if no free number in list in range"""

        self.assertEqual(
            -1, utils.get_free_number(
                50, 55, [48, 50, 51, 52, 53, 54, 55, 55, 55, 56]
            )
        )


class FillListTests(unittest.TestCase):

    def test_fill_list(self):
        """fills a list with members from another list until count is reached"""
        in_list = [5, 6, 7, 8, 9, 10]
        out_list = [1, 2, 3, 4]

        utils.fill_list(in_list, out_list, 6)

        self.assertListEqual(out_list, [1, 2, 3, 4, 5, 6])

    def test_fill_list_long_enough(self):
        """doesn't append anything to a list that long enough initially"""

        in_list = [6, 8, 9]
        out_list = [1, 2, 3, 4, 5]

        utils.fill_list(in_list, out_list, 3)

        self.assertListEqual(out_list, out_list)


class AddRcEntryTests(unittest.TestCase):

    def test_add_rc_entry(self):
        """appends a new entry to an empty return code"""
        fn_rc = []
        err_no = DME.DM_ESTORAGE
        err_message = "I'm sorry Dave, I'm afraid I can't do that"
        utils.add_rc_entry(fn_rc, err_no, err_message)
        self.assertListEqual(fn_rc, [[err_no, err_message, []]])

    def test_add_rc_entry_single_arg(self):
        """appends a new entry with a args to an empty return code"""
        fn_rc = []
        err_no = DME.DM_EINVAL
        err_message = "Can't touch this"
        args = [["key", "value"]]
        utils.add_rc_entry(fn_rc, err_no, err_message, args)
        self.assertListEqual(fn_rc, [[err_no, err_message, args]])

    @mock.patch("drbdmanage.utils.logging")
    def test_add_rc_entry_bad_message(self, mock_logging):
        """writes error to log on bad argument types"""
        fn_rc = []
        err_no = DME.DM_ENAME
        err_message = 13
        utils.add_rc_entry(fn_rc, err_no, err_message)
        self.assertTrue(mock_logging.error.called)

    @mock.patch("drbdmanage.utils.logging")
    def test_add_rc_entry_bad_args(self, mock_logging):
        """writes error to log if args key value pairs are not a list"""
        fn_rc = []
        err_no = DME.DM_ERESFILE
        err_message = "oops"
        args = ["--force", "true"]
        utils.add_rc_entry(fn_rc, err_no, err_message, args)
        self.assertTrue(mock_logging.error.called)

    @mock.patch("drbdmanage.utils.logging")
    def test_add_rc_entry_bad_key(self, mock_logging):
        """writes error to log if keys are not strings"""
        fn_rc = []
        err_no = DME.DM_ERESFILE
        err_message = "oops"
        args = [[1, "loneliest number"]]
        utils.add_rc_entry(fn_rc, err_no, err_message, args)
        self.assertTrue(mock_logging.error.called)

    @mock.patch("drbdmanage.utils.logging")
    def test_add_rc_entry_bad_value(self, mock_logging):
        """writes error to log if values aren't str, int, float, dbus.strig"""
        fn_rc = []
        err_no = DME.DM_ERESFILE
        err_message = "oops"
        args = [["nodes", ["figaro", "don juan"]]]
        utils.add_rc_entry(fn_rc, err_no, err_message, args)
        self.assertTrue(mock_logging.error.called)

if __name__ == "__main__":
    unittest.main()
