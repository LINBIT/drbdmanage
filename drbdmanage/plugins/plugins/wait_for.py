import logging
import time


import drbdmanage.exceptions as dm_exc
import drbdmanage.utils as dm_utils


last_ratio_point5_warning = 0


class PolicyBase(object):
    """Policy base class.

    This checks whether some policy is fulfilled - if majority
    of replicas are available, etc.

    policy can currently be
      * "ratio", "0.xxx"
        checks whether the given ratio of diskful deployments have been done.
        A value of 0.5 means that a majority of nodes are available;
        with two nodes *both*.
      * "count", "N"
        require that "N" diskful deployments are done.

    The return dict includes a 'result' string containing the boolean
    information.


    Please note that this does *not* imply that these are UpToDate, too;
    to check for data redundancy use these arguments:

      * "redundancy", "N"
        "N" copies must be available, ie. "N" nodes with UpToDate data.
        NOT IMPLEMENTED YET (needs DRBD sync status)


    Additionally, this plugin takes additional arguments 'starttime' and
    'timeout' (seconds since epoch, and timespan in seconds); if these are
    passed in, 'timeout' may be returned as 'true' in the result dict, too.

    """

    def __init__(self, server):
        self._server = server
        self._conf = {}

    def get_default_config(self):
        return {}

    def get_config(self):
        return self._conf

    # has to return a bool
    def set_config(self, config):
        self._conf = config
        return True

    def filter_good(self, list):
        """From a list of inputs, return "good" ones."""
        raise RuntimeError("Virtual method")

    @staticmethod
    def QD_ratio(self, srv, xlist, _ratio):

        ratio = float(_ratio)

        # warn for 0.5, but only once a minute (to silence waiting loops)
        global last_ratio_point5_warning
        if ((ratio == 0.5) and
                (time.time() > last_ratio_point5_warning + 60)):
            last_ratio_point5_warning = time.time()
            logging.warning("DRBDmanage wait-for plugin: a 'ratio' policy with 0.5 "
                            "means that one of two nodes is sufficient; "
                            "this is probably not what you want.")

        total = len(xlist)
        okay = self.filter_good(xlist)

        return len(okay) >= total * ratio

    @staticmethod
    def QD_count(self, srv, xlist, _count):

        c = int(_count)
        okay = self.filter_good(xlist)
        return len(okay) >= c

    def success(self, fmt, fmtpar, result=False, timeout=False, **other_res):
        d = {'result': dm_utils.bool_to_string(result),
             'timeout': dm_utils.bool_to_string(timeout)}
        if other_res:
            for k, v in other_res.items():
                d[str(k)] = str(v)
        rc = []
        dm_utils.add_rc_entry(rc, dm_exc.DM_SUCCESS, fmt, fmtpar)
        return (rc, d)

    def check_timeout(self, cnf):
        start = cnf.get('starttime', None)
        timeo = cnf.get('timeout', None)
        if start and timeo:
            now = time.time()
            end = float(start) + float(timeo)

            if now >= end:
                return self.success("""Timed out: %(start)s + %(t_o)s before now \
                                    (%(now)s), end was %(end)s""",
                                    [['start', start], ['t_o', timeo],
                                     ['now', str(now)], ['end', str(end)]],
                                    timeout=True)

        return None

    def check_policy(self, res_name, config, assignments):

        # wrong message for snapshots

        if len(assignments) == 0:
            return ([(dm_exc.DM_ENOENT,
                      'Resource %(res)s has no diskful assignments',
                      [['res', res_name]])],
                    {})

        # Main code
        # Store last checked policy
        policies = None

        # Try to find a policy acknowledging the current state
        for policy in ('ratio', 'count', 'redundancy'):
            arg = config.get(policy, None)
            if not arg:
                continue

            policies = policy

            func_name = "QD_" + policy
            func = getattr(self, func_name)
            res = func(self, self._server, assignments, arg)

            if res:
                return self.success('Policy %(pol)s says OK for resource %(res)s',
                                    [['pol', policy], ['res', res_name]],
                                    policy=policy,
                                    result=True)

        if not policies:
            return ([(dm_exc.DM_ENOTIMPL,
                      'No known policy given', [])],
                    {})

        return self.success('No policy (last was %(pol)s) acknowledged deployment \
                            for resource %(res)s',
                            [['pol', policies], ['res', res_name]],
                            policy=policies,
                            result=False)

    def get_resource(self, res_name):
        res = self._server._resources.get(res_name)
        if not res:
            return ([(dm_exc.DM_ENOENT,
                      'Resource %(res)s not found',
                      [['res', res_name]])],
                    {}), False
        return res, True


class WaitForResource(PolicyBase):
    """Ask whether policy for a resource is fulfilled.

    Like query_deployment, but for snapshots; the input dict
    needs to have a 'snapshot' item, too.

    Optionally you can pass an integer 'volnr' (which defaults to 'all');
    be careful with a 'count'
    """

    def _get_assignments(self, res, vol_spec):

        diskful = []

        for a in res.iterate_assignments():
            tstate = a.get_tstate()

            if dm_utils.is_set(tstate, a.FLAG_DISKLESS):
                continue

            for vol_state in a.iterate_volume_states():
                vid = vol_state.get_id()

                # Only "interesting" volumes
                if isinstance(vol_spec, long):
                    if vid != vol_spec:
                        continue

                while (len(diskful) <= vid):
                    diskful.append([])

                diskful[vid].append(dm_utils.is_set(vol_state.get_cstate(), vol_state.FLAG_DEPLOY) and
                                    dm_utils.is_set(vol_state.get_tstate(), vol_state.FLAG_DEPLOY))


        # look for the most scarce volume, and return its statistics
        min_depl = 1000
        ret = []
        for v, deployed_l in enumerate(diskful):
            deployed = sum(deployed_l)
            assert(deployed >= 0 and deployed <= 63)

            if deployed < min_depl:
                ret = deployed_l
                min_depl = deployed

        return ret

    def filter_good(self, assg):
        return [a for a in assg if a]

    def run(self):

        # Get config
        cnf = self._conf

        res_name = cnf['resource']

        res, okay = self.get_resource(res_name)
        if not okay:
            return res

        vols = cnf.get('volnr', '')
        if vols == '':
            vols = 'all'
        if vols != 'all':
            # might throw a ValueError, if the input is wrong
            vols = int(vols, base=10)

        # Check timeout
        to = self.check_timeout(cnf)
        if to:
            return to

        # Get list of diskful assignments
        diskful = self._get_assignments(res, vols)

        return self.check_policy(res_name, cnf, diskful)


class WaitForSnapshot(PolicyBase):
    """Ask whether policy for snapshot is fulfilled.

    Like query_deployment, but for snapshots; the input dict
    needs to have a 'snapshot' item, too.
    """

    def _get_assignments(self, snap):
        snaps = [s for s in snap.iterate_snaps_assgs()]
        return snaps

    def filter_good(self, xlist):
        return [a for a in xlist if a.is_deployed()]

    def run(self):

        # Get config
        cnf = self._conf

        res_name = cnf['resource']
        snap_name = cnf['snapshot']

        res, okay = self.get_resource(res_name)
        if not okay:
            return res

        # Check timeout
        to = self.check_timeout(cnf)
        if to:
            return to

        snap = res.get_snapshot(snap_name)
        if not snap:
            return ([(dm_exc.DM_ENOENT,
                      'Snapshot "%(sn)s" in resource "%(res)s" not found',
                      [['res', res_name], ['sn', snap_name]])],
                    {})

        snaps = self._get_assignments(snap)

        return self.check_policy(res_name, cnf, snaps)


class WaitForVolumeSize(PolicyBase):
    """Check whether resize_volume is done.

    Needs 'resource', 'volnr' and 'req_size' (in KB) as inputs.
    """

    def run(self):

        # Get config
        cnf = self._conf

        res_name = cnf['resource']
        volnr = int(cnf['volnr'])
        req_size = int(cnf['req_size'])

        res, okay = self.get_resource(res_name)
        if not okay:
            return res

        vol = res.get_volume(volnr)
        if not vol:
            return ([(dm_exc.DM_ENOENT,
                      'Volume "%(res)s/%(vol)s" in not found',
                      [['res', res_name], ['vol', str(volnr)]])],
                    {})

        # Check timeout
        to = self.check_timeout(cnf)
        if to:
            return to

        size = vol.get_size_kiB()
        # TODO(LINBIT): using ">=" means that shrinking is not supported (yet)
        return self.success("size is %(sz)s, req %(rq)s",
                            [['sz', str(size)], ['rq', str(req_size)]],
                            policy="size",
                            result=(size >= req_size))
