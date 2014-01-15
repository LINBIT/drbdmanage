#!/usr/bin/python

from distutils.core import setup

setup(
    name='drbdmanage',
    version='0.10',
    description='DRBD distributed resource management utility',
    long_description=
'Drbdmanage is a daemon and a command line utility that manages DRBD\n' +
'replicated LVM volumes across a group of machines.\n' +
'It maintains DRBD configuration an the participating machines. It\n' +
'creates/deletes the backing LVM volumes. It automatically places\n' +
'the backing LVM volumes among the participating machines.',
    author='Robert Altnoeder',
    author_email='robert.altnoeder@linbit.com',
    maintainer='LINBIT HA Solutions GmbH',
    maintainer_email='drbd-dev@lists.linbit.com',
    url='http://oss.linbit.com/drbdmanage',
    license='GPLv3',
    packages=[
        'drbdmanage',
        'drbdmanage.conf',
        'drbdmanage.drbd',
        'drbdmanage.storage'],
    scripts=['drbdmanaged.py', 'drbdmanage.py', 'drbdctrl_init.py'],
    data_files=[('/etc/drbd.d', ['conf/drbdctrl.res',
                                 'conf/drbdmanage-resources.res']),
                ('/etc', ['conf/drbdmanaged.conf', 'conf/drbdmanaged-lvm.conf']),
                ('/etc/dbus-1/system.d', ['conf/org.drbd.drbdmanaged.conf']),
                ]
    )
