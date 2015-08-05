#!/usr/bin/env python2

class LvmCheckFailedException(Exception):

    """
    Indicates failure to check for existing logical volumes.
    Not to be exposed to other parts of drbdmanage.
    """

    def __init__(self):
        super(LvmCheckFailedException, self).__init__()


class LvmException(Exception):

    """
    Indicates failure during the execution of Lvm internal functions.
    Not to be exposed to other parts of drbdmanage.
    """

    def __init__(self):
        super(LvmException, self).__init__()


class LvmUnmanagedVolumeException(Exception):

    """
    Indicates the attempt to operate on a volume not managed by drbdmanage
    """

    def __init__(self):
        super(LvmUnmanagedVolumeException, self).__init__()
