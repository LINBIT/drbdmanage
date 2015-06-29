#!/usr/bin/env python2

class LvmNgCheckFailedException(Exception):

    """
    Indicates failure to check for existing logical volumes.
    Not to be exposed to other parts of drbdmanage.
    """

    def __init__(self):
        super(LvmNgCheckFailedException, self).__init__()


class LvmNgException(Exception):

    """
    Indicates failure during the execution of LvmNg internal functions.
    Not to be exposed to other parts of drbdmanage.
    """

    def __init__(self):
        super(LvmNgException, self).__init__()


class LvmNgUnmanagedVolumeException(Exception):

    """
    Indicates the attempt to operate on a volume not managed by drbdmanage
    """

    def __init__(self):
        super(LvmNgUnmanagedVolumeException, self).__init__()
