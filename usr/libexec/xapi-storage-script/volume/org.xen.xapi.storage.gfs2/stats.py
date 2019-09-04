#!/usr/bin/env python

from __future__ import division
import errno

from xapi.storage.libs.libcow.callbacks import VolumeContext
from xapi.storage.libs.rrddlib import Datasource, PluginControl
from xapi.storage.libs.libcowi.lock import Lock
from xapi.storage.libs.cow_coalesce import touch
from sr import Implementation


def force_unlink(path):
    from os import unlink

    try:
        unlink(path)
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            raise


def get_gfs2_callbacks():
    from gfs2 import Callbacks
    return Callbacks()


def demonize():
    from os import close as os_close
    for fd in [0, 1, 2]:
        try:
            os_close(fd)
        except OSError:
            pass


def create_datasource_dict(stats_dict, scsi_id):
    """Create the Datasources to be reported for the SR

    The Datasource dictionary is created based
    on the available stats provided by each SR
    """

    ds_dict = {}
    if 'total_space' in stats_dict and 'free_space' in stats_dict:
        ds_dict['utilization'] = Datasource(
            'utilization_' + scsi_id,
            0.0,
            'float',
            description='SR ' + scsi_id + ' utilization',
            datasource_type='absolute',
            min_val=0.0,
            max_val=100.0,
            units='(fraction)',
            # owner='sr ' + sr_uuid
            owner='host'
        )

    if 'overprovision' in stats_dict:
        ds_dict['overprovision'] = Datasource(
            'overprovision_' + scsi_id,
            0.0,
            'float',
            description='SR ' + scsi_id + ' overprovision',
            datasource_type='absolute',
            min_val=0.0,
            max_val='inf',
            units='(fraction)',
            owner='host'
        )

    return ds_dict


def _get_utilization(stats_dict):
    return ((stats_dict['total_space'] - stats_dict['free_space']) /
            stats_dict['total_space'] * 100)


def _get_overprovision(stats_dict):
    return stats_dict['overprovision']


get_reading = {
    'utilization': _get_utilization,
    'overprovision': _get_overprovision,
}


def run_stats(uri):
    demonize()

    cb = get_gfs2_callbacks()
    with VolumeContext(cb, uri, 'w') as opq:
        scsi_id = cb.getUniqueIdentifier(opq).split('/')[-1]

    with Lock(opq, "stats", cb):
        sr_stats_plugin = PluginControl(
            'sr_stats_' + scsi_id,
            'Local',
            'Five_seconds',
            0.5
        )

        sr_impl = Implementation()
        ds_dict = create_datasource_dict(sr_impl.stat('', uri), scsi_id)

        # Calling full_update() is mandatory the first time
        # Creates memory mapped file and does all related prep work
        sr_stats_plugin.full_update(ds_dict)

        while True:
            # Wait 0.5 seconds before xcp-rrdd
            # is going to read the output file
            sr_stats_plugin.wake_up_before_next_reading()

            # Collect measurements
            # ---------------------- PUT CODE HERE ----------------------
            stats_dict = sr_impl.stat('', uri)

            for stat in ds_dict:
                ds_dict[stat].set_value(get_reading[stat](stats_dict))

            # -----------------------------------------------------------

            # As long as the datasources remain the same
            # (apart from their values) call fast_update()
            sr_stats_plugin.fast_update(ds_dict)


def start_stats(uri):
    import subprocess
    from pickle import dump as pickle_dump
    from os.path import join as path_join

    args = [('/usr/libexec/xapi-storage-script/volume/'
             'org.xen.xapi.storage.gfs2/stats.py'),
            uri]
    proc = subprocess.Popen(args)

    cb = get_gfs2_callbacks()
    with VolumeContext(cb, uri, 'w') as opq:
        stats_obj = path_join(
            "/var/run/sr-private",
            cb.getUniqueIdentifier(opq),
            'stats.obj'
        )

    touch(stats_obj)
    with open(stats_obj, 'w') as f:
        pickle_dump(proc, f)


def stop_stats(uri):
    from pickle import load as pickle_load
    from os.path import join as path_join

    cb = get_gfs2_callbacks()
    with VolumeContext(cb, uri, 'w') as opq:
        stats_obj = path_join(
            "/var/run/sr-private",
            cb.getUniqueIdentifier(opq),
            'stats.obj'
        )

    proc = None

    with open(stats_obj, 'r') as f:
        proc = pickle_load(f)

    proc.kill()
    force_unlink(stats_obj)


if __name__ == "__main__":
    from sys import argv
    uri = argv[1]
    run_stats(uri)
