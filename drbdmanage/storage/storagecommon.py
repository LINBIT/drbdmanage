#!/usr/bin/env python2

import drbdmanage.utils

class GenericStorage(object):

    _size_kiB = None


    def __init__(self, size_kiB):
        self._size_kiB = long(size_kiB)


    def get_size_kiB(self):
        """
        Returns the size of the volume in binary megabytes

        This is the size of the volume in units of (2 to the power of 10) bytes
        (bytes = size * 1024).

        @return: volume size in kiBiByte (2 ** 10 bytes, binary kilobytes)
        @rtype:  long
        """
        return self._size_kiB


    def get_size(self, unit):
        """
        Returns the size of the volume converted to the selected scale unit

        See the functions of the SizeCalc class in drbdmanage.utils for
        the unit selector constants.

        @return: volume size in the selected scale unit
        @rtype:  long
        """
        return drbdmanage.utils.SizeCalc.convert(
            self._size_kiB, drbdmanage.utils.SizeCalc.UNIT_kiB, unit
        )
