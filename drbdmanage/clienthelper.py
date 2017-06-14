"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2016 - 2017 LINBIT HA-Solutions GmbH
    Author: Roland Kammerer <roland.kammerer@linbit.com>

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

import dbus
import logging
import time
import os
import drbdmanage.consts as dm_const
import drbdmanage.exceptions as dm_exc
import drbdmanage.utils as dm_utils


def delay_for(seconds):
    time.sleep(seconds)


def l10n(text):
    return text


def lWarn(text):
    return text


def lInfo(text):
    return text


def lError(text):
    return text


class DrbdManageClientHelper(object):

    def __init__(self):
        self.empty_list = dbus.Array([], signature='a(s)')
        self.empty_dict = dbus.Array([], signature='a(ss)')

        # to overwrite
        logging.basicConfig()
        self.logger = logging.getLogger(__name__)

        self.dbus_connect()

    # methods you probably want to overwrite
    def _(self, text):
        return l10n(text)

    def _LW(self, text):
        return lWarn(text)

    def _LI(self, text):
        return lInfo(text)

    def _LE(self, text):
        return lError(text)

    def sleep(self, seconds):
        delay_for(seconds)

    # methods you probably should _not_ overwrite
    def dbus_connect(self, retries_max=15):
        self.odm = dbus.SystemBus().get_object(dm_const.DBUS_DRBDMANAGED,
                                               dm_const.DBUS_SERVICE)
        self.odm.ping()

        tries = 0
        while tries < retries_max:
            server_rc = self.odm.wait_for_startup()
            if not dm_utils.is_rc_retry(server_rc[0]):
                break
            tries += 1
            time.sleep(2)
        # no consequences needed here, the next call to call_or_reconnect handles the answer

    def call_or_reconnect(self, fn, *args):
        """Call DBUS function; on a disconnect try once to reconnect."""
        try:
            return fn(*args)
        except dbus.DBusException as e:
            self.logger.warning(self._LW('Got disconnected; trying to reconnect. (%s)') % e)
            self.dbus_connect()
            # Old function object is invalid, get new one.
            return getattr(self.odm, fn._method_name)(*args)

    def _fetch_answer_data(self, res, key, level=None, req=True):
        for code, fmt, data in res:
            if code == dm_exc.DM_INFO:
                if level and level != fmt:
                    continue

                value = [v for k, v in data if k == key]
                if value:
                    if len(value) == 1:
                        return value[0]
                    else:
                        return value

        if req:
            if level:
                l = level + ":" + key
            else:
                l = key

            msg = self._('DRBDmanage driver error: expected key "%s" '
                         'not in answer, wrong DRBDmanage version?') % l
            self.logger.error(msg)
            raise dm_exc.ClientHelperException('_fetch_answer_data', msg)

        return None

    def _check_result(self, res, ignore=None, ret=0):
        seen_success = False
        seen_error = False
        result = ret
        for (code, fmt, arg_l) in res:
            # convert from DBUS to Python
            arg = dict(arg_l)
            if ignore and code in ignore:
                if not result:
                    result = code
                continue
            if code == dm_exc.DM_SUCCESS:
                seen_success = True
                continue
            if code == dm_exc.DM_INFO:
                continue
            try:
                seen_error = self._("Received error string: %s") % (fmt % arg)
            except:
                seen_error = self._('Unknown error')

        if seen_error:
            raise dm_exc.ClientHelperException('_check_result', seen_error)
        if seen_success:
            return ret
        # by default okay - or the ignored error code.
        return ret

    def _call_policy_plugin(self, plugin, pol_base, pol_this):
        """Returns True for done, False for timeout."""

        pol_inp_data = dict(pol_base)
        pol_inp_data.update(pol_this,
                            starttime=str(time.time()))

        retry = 0
        while True:
            res, pol_result = self.call_or_reconnect(
                self.odm.run_external_plugin,
                plugin,
                pol_inp_data)
            self._check_result(res)

            if pol_result['result'] == dm_const.BOOL_TRUE:
                return True

            if pol_result['timeout'] == dm_const.BOOL_TRUE:
                return False

            self.sleep(min(0.5 + retry / 5, 2))
            retry += 1

    def create_volume(self, res_name, props=None,
                      deploy_hosts=[], deploy_count=1, size=100*1024):
        """Creates a DRBD resource.
        If deploy_hosts is set, deploy the resource to these hosts, deploy_count is ignored
        If deploy_hosts is [], the resouces will be autodeployed to deploy_count hosts.

        The rest of nodes in the cluster will get client assignments.
        """
        if not props:
            props = self.empty_dict

        res = self.call_or_reconnect(self.odm.create_resource,
                                     res_name, props)
        self._check_result(res, ignore=[dm_exc.DM_EEXIST], ret=None)

        res = self.call_or_reconnect(self.odm.create_volume,
                                     res_name, size, props)
        self._check_result(res)
        drbd_vol = self._fetch_answer_data(res, dm_const.VOL_ID)

        if not deploy_hosts:
            res = self.call_or_reconnect(self.odm.auto_deploy,
                                         res_name, deploy_count, 0, True)
            try:
                self._check_result(res)
            except:
                return False
        else:
            for node_name in deploy_hosts:
                res = self.call_or_reconnect(self.odm.assign,
                                             node_name,
                                             res_name,
                                             self.empty_dict)
                self._check_result(res)
            #  make the rest clients
            res, nl = self.call_or_reconnect(self.odm.list_nodes,
                                             self.empty_list,
                                             0,
                                             self.empty_dict,
                                             self.empty_dict)

            delta = len(nl) - len(deploy_hosts)
            if delta > 0:
                nodes = [n[0] for n in nl]
                nodes = [n for n in nodes if n not in deploy_hosts]
                for node_name in nodes:
                    res = self.call_or_reconnect(self.odm.assign,
                                                 node_name,
                                                 res_name,
                                                 [(dm_const.FLAG_DISKLESS,
                                                   dm_const.BOOL_TRUE)]
                                                 )
                    self._check_result(res)

        self.odm.resume_all()
        okay = self._call_policy_plugin('drbdmanage.plugins.plugins.wait_for.WaitForResource',
                                        {'ratio': '0.51', 'timeout': '60'},
                                        dict(resource=res_name, volnr=str(drbd_vol)))
        return okay

    def delete_volume(self, res_name, vol_nr=0):
        """Deletes a resource."""
        d_res_name, d_vol_nr = res_name, vol_nr

        if not d_res_name:
            # OK, already gone.
            return True

        # TODO(PM): check if in use? Ask whether Primary, or just check result?
        res = self.call_or_reconnect(self.odm.remove_volume,
                                     d_res_name, d_vol_nr, False)
        self._check_result(res, ignore=[dm_exc.DM_ENOENT])

        # Ask for volumes in that resource that are not scheduled for deletion.
        res, rl = self.call_or_reconnect(self.odm.list_volumes,
                                         [d_res_name],
                                         0,
                                         [(dm_const.TSTATE_PREFIX +
                                           dm_const.FLAG_REMOVE,
                                           dm_const.BOOL_FALSE)],
                                         self.empty_list)
        self._check_result(res)

        # We expect the _resource_ to be here still (we just got a volnr from
        # it!), so just query the volumes.
        # If the resource has no volumes anymore, the current DRBDmanage
        # version (errorneously, IMO) returns no *resource*, too.
        if len(rl) > 1:
            message = self._('DRBDmanage expected one resource ("%(res)s"), '
                             'got %(n)d') % {'res': d_res_name, 'n': len(rl)}
            raise dm_exc.ClientHelperException('delete_volume', message)

        # Delete resource, if empty
        if (not rl) or (not rl[0]) or (len(rl[0][2]) == 0):
            res = self.call_or_reconnect(self.odm.remove_resource,
                                         d_res_name, False)
            self._check_result(res, ignore=[dm_exc.DM_ENOENT])

        return True

    def res_exists(self, res_name, props=None):
        if not props:
            props = self.empty_dict
        res, rl = self.call_or_reconnect(self.odm.list_resources,
                                         [res_name],
                                         0,
                                         props,
                                         self.empty_dict)

        self._check_result(res, ignore=[dm_exc.DM_ENOENT])
        return len(rl) == 1

    def list_resource_names(self, props=None):
        if not props:
            props = self.empty_dict
        res, rl = self.call_or_reconnect(self.odm.list_resources,
                                         self.empty_list,
                                         0,
                                         props,
                                         self.empty_dict)
        self._check_result(res)
        return [r[0] for r in rl]

    def local_path(self, res_name, vol_nr=0):
        d_res_name, d_vol_nr = res_name, vol_nr

        res, data = self.call_or_reconnect(self.odm.text_query,
                                           [dm_const.TQ_GET_PATH,
                                            d_res_name,
                                            str(d_vol_nr)])
        self._check_result(res)

        if len(data) == 1:
            return data[0]

        message = self._('Got bad path information from DRBDmanage! (%s)') % data
        raise dm_exc.ClientHelperException('local_path', message)

    def is_locally_mounted(self, mountpoint):
        mountpoint = os.path.abspath(mountpoint)
        ret = False
        with open('/proc/mounts') as fp:
            for line in fp:
                if line.split()[1] == mountpoint:
                    ret = True
                    break
        return ret
