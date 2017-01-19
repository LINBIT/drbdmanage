#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013 - 2017  LINBIT HA-Solutions GmbH
                               Author: R. Altnoeder, Roland Kammerer

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
DRBD meta data related functions and calculations
"""

import logging
import drbdmanage.consts as consts
import drbdmanage.exceptions as dmexc
import drbdmanage.utils as utils

class MetaData(object):

    # Alignment of the metadata area
    DRBD_MD_ALIGN_kiB = int(4)

    # Alignment (or granularity) of the bitmap area
    DRBD_BM_ALIGN_kiB = int(4)

    # Alignment (or granularity) in bytes of the bitmap for a single peer.
    # The bitmap size is increased or decreased in steps of DRBD_BM_PEER_ALIGN bytes.
    DRBD_BM_PEER_ALIGN = int(8)

    # Data size in kiB covered by one bitmap bit
    DRBD_BM_BIT_COVER_kiB = int(4)

    # Data size covered by one bitmap byte
    DRBD_BM_BYTE_COVER_kiB = int(32)

    # Default size of the activity log
    DRBD_DEFAULT_AL_kiB = int(32)

    # Minimum size of the activity log
    DRBD_MIN_AL_kiB = int(4)

    # Maximum size of the activity log
    DRBD_MAX_AL_kiB = int(1048576)

    # Alignment of the activity log
    DRBD_AL_ALIGN_kiB = int(4)

    # Size of the DRBD meta data superblock
    DRBD_MD_SUPERBLK_kiB = int(4)

    # Maximum size in kiB of a DRBD-replicated device
    # Must be a multiple of DRBD_BM_BIT_COVER_kiB
    DRBD_MAX_kiB = long(1) << 40

    # Minimum gross size (including metadata) of a DRBD-replicated device
    DRBD_MIN_GROSS_kiB = long(68)

    # Minimum net size (without metadata) of a DRBD-replicated device
    DRBD_MIN_NET_kiB = long(4)

    # Minimum number of peers
    DRBD_MIN_PEERS = int(1)

    # Maximum number of peers
    DRBD_MAX_PEERS = int(31)

    # Default number of activity log stripes
    DEFAULT_AL_STRIPES = 1

    # Default size of the activity log (or default stripe size)
    DEFAULT_AL_kiB = 32


    @classmethod
    def get_net_kiB(cls, gross_kiB, peers, al_stripes, al_stripe_kiB):
        """
        Returns the net size that can be provided by a backend storage with the specified gross size
        """
        gross_kiB     = long(gross_kiB)
        peers         = int(peers)
        al_stripes    = int(al_stripes)
        al_stripe_kiB = int(al_stripe_kiB)

        net_kiB = 0

        bitmap_kiB = MetaData._get_bitmap_internal_kiB_gross(gross_kiB, peers)
        al_kiB = MetaData._get_al_kiB(al_stripes, al_stripe_kiB)

        md_kiB = utils.align_up(
            bitmap_kiB + al_kiB + MetaData.DRBD_MD_SUPERBLK_kiB,
            MetaData.DRBD_MD_ALIGN_kiB
        )
        gross_kiB = utils.align_down(gross_kiB, MetaData.DRBD_BM_BIT_COVER_kiB)
        if md_kiB < gross_kiB:
            net_kiB = gross_kiB - md_kiB
        else:
            raise MinSizeException(
                "The specified DRBD volume gross size (%d kiB) is too small to keep the volume's meta data area"
                % (gross_kiB)
            )

        return net_kiB

    @classmethod
    def get_gross_kiB(cls, net_kiB, peers, al_stripes, al_stripe_kiB):
        """
        Returns the backend storage gross size required to provide the specified net size
        """
        net_kiB       = long(net_kiB)
        peers         = int(peers)
        al_stripes    = int(al_stripes)
        al_stripe_kiB = int(al_stripe_kiB)

        # BEGIN HOTFIX
        # FIXME: Hotfix for resizing restored snapshots
        #        after reducing the number of maximum peers
        peers = consts.HOTFIX_MAX_PEERS
        # END HOTFIX

        gross_kiB = 0

        bitmap_kiB = MetaData._get_bitmap_internal_kiB_net(
            net_kiB, peers, al_stripes, al_stripe_kiB
        )

        al_kiB = MetaData._get_al_kiB(al_stripes, al_stripe_kiB)
        md_kiB = utils.align_up(
            bitmap_kiB + al_kiB + MetaData.DRBD_MD_SUPERBLK_kiB,
            MetaData.DRBD_MD_ALIGN_kiB
        )
        gross_kiB = utils.align_up(net_kiB, MetaData.DRBD_BM_BIT_COVER_kiB) + md_kiB

        check_max_drbd_kiB(gross_kiB)
        if gross_kiB < MetaData.DRBD_MIN_GROSS_kiB:
            gross_kiB = MetaData.DRBD_MIN_GROSS_kiB

        return gross_kiB

    @classmethod
    def get_internal_md_kiB(cls, is_net_size, size_kiB, peers, al_stripes, al_stripe_kiB):
        """
        Returns the amount of storage required for internal meta data
        """
        if type(is_net_size) is not bool:
            logging.error(
                "Implementation error: Incorrect use of "
                "drbdmanage.drbd.metadata.MetaData.get_internal_md_kiB"
            )
            raise dmexc.DebugException
        size_kiB      = long(size_kiB)
        peers         = int(peers)
        al_stripes    = int(al_stripes)
        al_stripe_kiB = int(al_stripe_kiB)

        size_kiB = utils.align_up(size_kiB, MetaData.DRBD_BM_BIT_COVER_kiB)

        al_kiB = MetaData._get_al_kiB(al_stripes, al_stripe_kiB)
        md_kiB = 0
        if is_net_size:
            bitmap_kiB = MetaData._get_bitmap_internal_kiB_net(
                size_kiB, peers, al_stripes, al_stripe_kiB
            )
            md_kiB = utils.align_up(
                al_kiB + bitmap_kiB + MetaData.DRBD_MD_SUPERBLK_kiB,
                MetaData.DRBD_MD_ALIGN_kiB
            )
        else:
            bitmap_kiB = MetaData._get_bitmap_internal_kiB_gross(size_kiB, peers)
            md_kiB = utils.align_up(
                al_kiB + bitmap_kiB + MetaData.DRBD_MD_SUPERBLK_kiB,
                MetaData.DRBD_MD_ALIGN_kiB
            )

        return md_kiB

    @classmethod
    def get_external_md_kiB(cls, size_kiB, peers, al_stripes, al_stripe_kiB):
        """
        Returns the amount of storage required for external meta data
        """
        size_kiB      = long(size_kiB)
        peers         = int(peers)
        al_stripes    = int(al_stripes)
        al_stripe_kiB = int(al_stripe_kiB)

        md_kiB = 0

        check_max_drbd_kiB(size_kiB)
        check_peers(peers)

        size_kiB = utils.align_up(size_kiB, MetaData.DRBD_BM_BIT_COVER_kiB)

        al_kiB = MetaData._get_al_kiB(al_stripes, al_stripe_kiB)
        bitmap_kiB = MetaData._get_bitmap_external_kiB(size_kiB, peers)
        md_kiB = utils.align_up(
            al_kiB + bitmap_kiB + MetaData.DRBD_MD_SUPERBLK_kiB,
            MetaData.DRBD_MD_ALIGN_kiB
        )

        return md_kiB

    @classmethod
    def _get_bitmap_external_kiB(cls, size_kiB, peers):
        size_kiB      = long(size_kiB)
        peers         = int(peers)

        check_max_drbd_kiB(size_kiB)
        check_peers(peers)

        size_kiB = utils.align_up(size_kiB, MetaData.DRBD_BM_BIT_COVER_kiB)

        bitmap_peer_b = utils.ceiling_divide(size_kiB, MetaData.DRBD_BM_BYTE_COVER_kiB)
        bitmap_peer_b = utils.align_up(bitmap_peer_b, MetaData.DRBD_BM_PEER_ALIGN)
        bitmap_b = bitmap_peer_b * peers
        bitmap_kiB = utils.align_up(
            utils.align_up(bitmap_b, 1024) / 1024,
            MetaData.DRBD_BM_ALIGN_kiB
        )

        return bitmap_kiB

    @classmethod
    def _get_bitmap_internal_kiB_net(cls, net_kiB, peers, al_stripes, al_stripe_kiB):
        net_kiB       = long(net_kiB)
        peers         = int(peers)
        al_stripes    = int(al_stripes)
        al_stripe_kiB = int(al_stripe_kiB)

        check_min_drbd_kiB_net(net_kiB)
        check_max_drbd_kiB(net_kiB)
        check_peers(peers)

        al_kiB = MetaData._get_al_kiB(al_stripes, al_stripe_kiB)

        # Base size for the recalculation of the gross size
        # in each iteration of the sequence limit loop
        # The base size is the net data + activity log + superblock,
        # but without the bitmap, therefore:
        # base_size_kiB + bitmap_kiB == gross_kiB
        base_kiB = net_kiB + al_kiB + MetaData.DRBD_MD_SUPERBLK_kiB

        # Calculate the size of the bitmap required to cover the
        # gross size of the device, which includes the size of the bitmap.
        gross_kiB = base_kiB
        bitmap_kiB = 0
        bitmap_cover_kiB = 0
        while bitmap_cover_kiB < gross_kiB:
            # Bitmap size required to cover the gross size on each peer
            bitmap_peer_b = utils.ceiling_divide(gross_kiB, MetaData.DRBD_BM_BYTE_COVER_kiB)
            # Align to the per-peer bitmap granularity
            bitmap_peer_b = utils.align_up(bitmap_peer_b, MetaData.DRBD_BM_PEER_ALIGN)

            # Bitmap size for all peers
            bitmap_b = bitmap_peer_b * peers

            # Gross size covered by the current bitmap size
            bitmap_cover_kiB = bitmap_peer_b * MetaData.DRBD_BM_BYTE_COVER_kiB

            # Actual size of the bitmap in the DRBD metadata area (after alignment)
            bitmap_kiB = utils.align_up(
                utils.align_up(bitmap_b, 1024) / 1024,
                MetaData.DRBD_BM_ALIGN_kiB
            )

            # Resulting gross size after including the bitmap size
            gross_kiB = base_kiB + bitmap_kiB

        check_max_drbd_kiB(gross_kiB)

        return bitmap_kiB

    @classmethod
    def _get_bitmap_internal_kiB_gross(cls, gross_kiB, peers):
        gross_kiB = long(gross_kiB)
        peers     = int(peers)

        check_min_drbd_kiB_gross(gross_kiB)
        check_max_drbd_kiB(gross_kiB)
        check_peers(peers)

        gross_kiB = utils.align_up(gross_kiB, MetaData.DRBD_BM_BIT_COVER_kiB)

        bitmap_peer_b = utils.ceiling_divide(gross_kiB, MetaData.DRBD_BM_BYTE_COVER_kiB)
        bitmap_peer_b = utils.align_up(bitmap_peer_b, MetaData.DRBD_BM_PEER_ALIGN)
        bitmap_b = bitmap_peer_b * peers
        bitmap_kiB = utils.align_up(utils.align_up(bitmap_b, 1024) / 1024, MetaData.DRBD_BM_ALIGN_kiB)

        return bitmap_kiB

    @classmethod
    def _get_al_kiB(cls, al_stripes, al_stripe_kiB):
        al_stripes    = int(al_stripes)
        al_stripe_kiB = long(al_stripe_kiB)

        if al_stripes < 1:
            raise AlStripesException(
                "Number of activity log strips (%d) is smaller than the minimum of 1"
                % (al_stripes)
            )

        if al_stripe_kiB < 1:
            raise MinAlSizeException(
                "Activity log stripe size (%d kiB) is smaller than the minimum of 1 kiB"
                % (al_stripe_kiB)
            )

        if al_stripe_kiB > MetaData.DRBD_MAX_AL_kiB:
            raise MaxAlSizeException(
                "Activity log stripe size (%d kiB) is larger than the maximum of %d kiB"
                % (al_stripe_kiB, MetaData.DRBD_MAX_AL_kiB)
            )

        al_kiB = al_stripe_kiB * al_stripes
        al_kiB = utils.align_up(al_kiB, MetaData.DRBD_AL_ALIGN_kiB)

        if al_kiB < MetaData.DRBD_MIN_AL_kiB:
            raise MinAlSizeException(
                "Activity log total size (%d kiB) is smaller than the minimum of 1 kiB"
                % (al_stripe_kiB)
            )
        elif al_kiB > MetaData.DRBD_MAX_AL_kiB:
            raise MaxAlSizeException(
                "Activity log total size (%d kiB) is larger than the maximum of %d kiB"
                % (al_stripe_kiB, MetaData.DRBD_MAX_AL_kiB)
            )

        return al_kiB

def check_peers(peers):
    peers = int(peers)
    if peers < MetaData.DRBD_MIN_PEERS or peers > MetaData.DRBD_MAX_PEERS:
        raise PeerCountException(
            "Number of peers (%d) is out of range (%d - %d)"
            % (peers, MetaData.DRBD_MIN_PEERS, MetaData.DRBD_MAX_PEERS)
        )

def check_min_drbd_kiB_net(net_kiB):
    net_kiB = long(net_kiB)
    if net_kiB < MetaData.DRBD_MIN_NET_kiB:
        raise MinSizeException(
            "Specified DRBD data area size (%d kiB) is smaller than the minimum of %d kiB"
            % (net_kiB, MetaData.DRBD_MIN_NET_kiB)
        )

def check_min_drbd_kiB_gross(gross_kiB):
    gross_kiB = long(gross_kiB)
    if gross_kiB < MetaData.DRBD_MIN_GROSS_kiB:
        raise MinSizeException(
            "Specified DRBD volume gross size (%d kiB) is smaller than the minimum of %d kiB"
            % (gross_kiB, MetaData.DRBD_MIN_GROSS_kiB)
        )

def check_max_drbd_kiB(size_kiB):
    size_kiB = long(size_kiB)
    if size_kiB > MetaData.DRBD_MAX_kiB:
        raise MaxSizeException(
            "DRBD volume size (%d kiB) is larger than the maximum of %d kiB"
            % (size_kiB, MetaData.DRBD_MAX_kiB)
        )

class MetaDataException(dmexc.DrbdManageException):

    message = ""

    def __init__(self, message):
        super(MetaDataException, self).__init__()
        if message is not None:
            self.message = message
        self.error_code = dmexc.DM_EINVAL

    def add_rc_entry(self, fn_rc):
        error_text = dmexc.dm_exc_text(self.error_code)
        if len(self.message) > 0:
            error_text += ": " + self.message
        fn_rc.append([self.error_code, error_text, []])


class MinSizeException(MetaDataException):

    def __init__(self, message):
        super(MinSizeException, self).__init__(message)


class MaxSizeException(MetaDataException):

    def __init__(self, message):
        super(MaxSizeException, self).__init__(message)


class MinAlSizeException(MetaDataException):

    def __init__(self, message):
        super(MinAlSizeException, self).__init__(message)


class MaxAlSizeException(MetaDataException):

    def __init__(self, message):
        super(MaxAlSizeException, self).__init__(message)


class AlStripesException(MetaDataException):

    def __init__(self, message):
        super(AlStripesException, self).__init__(message)


class PeerCountException(MetaDataException):

    def __init__(self, message):
        super(PeerCountException, self).__init__(message)
