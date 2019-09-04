#!/usr/bin/env python

from __future__ import division
import copy
import os
import os.path
import re
import sys
import urlparse
import json
import pickle

import sqlite3

from transport import transport
import xapi.storage.api.v4.volume
from xapi.storage.common import call
from xapi.storage.libs.libcow.lock import Lock
from xapi.storage.libs.libcow.coalesce import COWCoalesce
from xapi.storage.libs.libcow.volume import COWVolume
from xapi.storage.libs import util
from xapi.storage import log

import dlm_monitor
import gfs2

mountpoint_root = "/var/run/sr-mount/"
MULTIPATH_FLAG = "/var/run/nonpersistent/multipath_enabled"
BLK_DEV_FILE = "block_device.pickle"


def getSRMountPath(dbg, sr_uuid):
    mnt_path = os.path.join(mountpoint_root, sr_uuid)
    util.mkdir_p(mnt_path)
    return mnt_path


def gfs2_mount(dbg, dev_path, mnt_path):
    # FIXME: Ensure corosync+dlm are configured and running
    if not os.path.ismount(mnt_path):
        cmd = ["/usr/sbin/modprobe", "gfs2"]
        call(dbg, cmd)

        cmd = ["/usr/bin/mount", "-t", "gfs2", "-o",
               "noatime,nodiratime", dev_path, mnt_path]
        try:
            call(dbg, cmd)
        except:
            raise


def gfs2_mount_local(dbg, dev_path, mnt_path):
    if not os.path.ismount(mnt_path):
        cmd = ["/usr/bin/mount", "-t", "gfs2", "-o",
               "noatime,nodiratime,lockproto=lock_nolock", dev_path, mnt_path]
        call(dbg, cmd)


def gfs2_umount(dbg, mnt_path):
    cmd = ["/usr/bin/umount", mnt_path]
    call(dbg, cmd)


def plug_device(dbg, config, rescan=False):
    """
    Activate the block device for the filesystem
    """
    block_device = transport.Transport.get_block(config, rescan)

    dev_path = block_device.path
    if not os.path.exists(dev_path):
        raise xapi.storage.api.v4.volume.Sr_not_attached(dev_path)
    return block_device


def unplug_device(dbg, device):
    """
    Deactivate the block device for the filesystem
    """
    # Use block device to cleanup transport and device level
    # connections
    device.close()


def load_config(config_json):
    return json.loads(config_json)


def get_block_device_path(sr_uuid):
    return os.path.join(util.var_run_prefix(), "sr", sr_uuid)


def save_block_device(dbg, sr_uuid, device):
    path = get_block_device_path(sr_uuid)
    try:
        os.makedirs(path)
    except OSError:
        pass
    path = os.path.join(path, BLK_DEV_FILE)
    with open(path, 'w+') as f:
        pickle.dump(device, f)


def block_device_exists(sr_uuid):
    path = get_block_device_path(sr_uuid)
    return os.path.exists(os.path.join(path, BLK_DEV_FILE))


def retrieve_block_device(dbg, sr_uuid, cleanup=False):
    path = get_block_device_path(sr_uuid)
    path = os.path.join(path, BLK_DEV_FILE)

    with open(path) as f:
        block_device = pickle.load(f)

    if cleanup:
        os.unlink(path)

    return block_device


@util.decorate_all_routines(util.log_exceptions_in_function)
class Implementation(xapi.storage.api.v4.volume.SR_skeleton):

    def _check_for_gfs2_sr(self, dbg, device):
        stdout, _, ret = call(dbg, ['/usr/sbin/tunegfs2', '-l', device.path],
                              error=False, simple=False)

        if ret == 0:
            match = re.search(r'File system UUID: (\S*)', stdout)

            sr_stat = {
                "sr": "",
                "name": "",
                "description": "",
                "total_space": device.size,
                "free_space": 0,
                "uuid": match.group(1),
                "overprovision": 0,
                "datasources": [],
                "clustered": True,
                "health": ["Healthy", ""]
            }
            return sr_stat

        return None

    def _create_dev_probe_result(self, dbg, configuration, dev):
        result = {}
        result['complete'] = True
        result['configuration'] = copy.deepcopy(configuration)
        result['extra_info'] = {}
        sr = self._check_for_gfs2_sr(dbg, dev)
        if sr:
            result['sr'] = sr
            result['configuration']['sr_uuid'] = sr['uuid']
        return result

    def _create_incomplete_probe_result(self, dbg, configuration, trans_exc):
        """
        Create a set of probe results from a transport exception
        """
        results = []
        json_response = json.loads(trans_exc.message)
        if 'options' in json_response:
            # We have some extra options available
            options = json_response['options']
            for option in options:
                for value in options[option]:
                    result = {}
                    result['complete'] = False
                    result['configuration'] = copy.deepcopy(configuration)
                    result['configuration'][option] = value['value']
                    result['extra_info'] = value['info']
                    results.append(result)
        else:
            # Just an error from the transport
            raise trans_exc

        return results

    def probe(self, dbg, configuration):
        """
        Probe for device configs and existing SRs
        """
        log.debug('{}: SR.probe: config=\'{}\''.format(dbg, configuration))

        result = []

        dev = None
        try:
            dev = plug_device(dbg, configuration, rescan=True)
        except transport.TransportException, trans_exc:
            log.debug('Transport exception in probe {}'.format(
                sys.exc_info()))
            result.extend(self._create_incomplete_probe_result(
                dbg, configuration, trans_exc))

        if dev:
            result.append(self._create_dev_probe_result(
                dbg, configuration, dev))
            unplug_device(dbg, dev)

        return result

    def attach(self, dbg, configuration):
        """
        Attach the block tranpsort for the SR and mount the FS
        """
        log.debug("%s: SR.attach: config='%s'" % (dbg, configuration))

        callbacks = gfs2.Callbacks()

        if 'sr_uuid' not in configuration:
            raise Exception()

        sr_uuid = configuration['sr_uuid']

        mnt_path = getSRMountPath(dbg, sr_uuid)
        sr_uri = "file://" + mnt_path

        # SR.attach is idempotent
        if os.path.ismount(mnt_path):
            log.debug("%s: SR.attach: config='%s' ALREADY ATTACHED" %
                      (dbg, configuration))
            return sr_uri

        if os.path.exists(MULTIPATH_FLAG):
            configuration["multipath"] = "yes"

        dev = plug_device(dbg, configuration)
        dev_path = dev.path

        call(dbg, ["/usr/sbin/modprobe", "gfs2"])

        # Mount the gfs2 filesystem
        gfs2_mount(dbg, dev_path, mnt_path)
        log.debug("%s: mounted on %s" % (dbg, mnt_path))

        # Save attached block device
        save_block_device(dbg, sr_uuid, dev)

        # Start GC for this host
        COWCoalesce.start_gc(dbg, "gfs2", sr_uri)

        dev.set_persistent()

        database = callbacks.get_database(mnt_path)

        with Lock(mnt_path, dlm_monitor.NODE_CLEANUP_LOCK, callbacks):
            # First remove this host from the database for cluster
            # outage recovery
            with database.write_context():
                database.remove_host_by_host_id(callbacks.get_current_host())

            # Add the host record to the database
            with database.write_context():
                database.add_host(
                    callbacks.get_current_host(),
                    callbacks.get_cluster_node_id(),
                    dev.size)

        database.close()

        return sr_uri

    def create(self, dbg, sr_uuid, configuration,
               name, description):
        log.debug("{}: SR.create: config={}".format(
            dbg, configuration))

        callbacks = gfs2.Callbacks()

        dev = plug_device(dbg, configuration)
        dev_path = dev.path
        mnt_path = getSRMountPath(dbg, sr_uuid)

        log.debug("{}: dev_path = {}".format(dbg, dev_path))

        try:
            util.raise_exc_if_device_in_use(dbg, dev_path)
        except:
            unplug_device(dbg, dev)
            raise

        cmd = ["/usr/sbin/corosync-cmapctl", "totem.cluster_name"]
        out = call(dbg, cmd).rstrip()
        # Cluster id is quite limited in size
        cluster_name = out.split("=")[1][1:]

        # Generate a UUID for the filesystem name
        # According to mkfs.gfs2 manpage, SR name can only be 1--16 chars in
        # length
        sr_name = sr_uuid[0:16]
        fsname = "{}:{}".format(cluster_name, sr_name)

        cmd = ["/usr/bin/dd", "if=/dev/zero", "of={}".format(dev_path),
               "bs=1M", "count=10", "oflag=direct"]
        call(dbg, cmd)

        journal_count = (configuration['journal_count']
                         if 'journal_count' in configuration else '16')
        journal_size = (configuration['journal_size']
                        if 'journal_size' in configuration else '128')

        # Make the filesystem
        cmd = ["/usr/sbin/mkfs.gfs2",
               "-t", fsname,
               "-p", "lock_dlm",
               "-J", journal_size,
               "-O",
               "-j", journal_count,
               "-K",  # CA-279915: Don't discard until kernel error is fixed
               dev_path]
        call(dbg, cmd)

        cmd = ["/usr/sbin/tunegfs2",
               "-U", sr_uuid,
               dev_path]
        call(dbg, cmd)

        # Temporarily mount the filesystem so we can write the SR metadata
        gfs2_mount_local(dbg, dev_path, mnt_path)

        # Create the metadata database
        callbacks.create_database(mnt_path)

        read_caching = True
        if 'read_caching' in configuration:
            if configuration['read_caching'] not in ['true', 't', 'on',
                                                     '1', 'yes']:
                read_caching = False

        configuration['sr_uuid'] = sr_uuid

        meta = {
            "sr_uuid": sr_uuid,
            "name": name,
            "description": description,
            "config_json": json.dumps(configuration),
            "unique_id": sr_uuid,
            "fsname": fsname,
            "read_caching": read_caching,
            "keys": {}
        }
        sr_uri = "file://" + mnt_path
        util.updateSRMetadata(dbg, sr_uri, meta)

        gfs2_umount(dbg, mnt_path)

        unplug_device(dbg, dev)

        return configuration

    def destroy(self, dbg, sr):
        meta = util.getSRMetadata(dbg, sr)
        self._detach(dbg, sr)

        config = load_config(meta["config_json"])
        dev = plug_device(dbg, config)
        try:
            # Best effort attempt to discard the entire backing device to:
            # - Actually free space used by the LUN on a potentially thin
            #   provisioned storage array
            # - Allow the storage array to potentially refresh the SSDs
            #   that backed the LUN
            dev_path = dev.path
            cmd = ["/usr/bin/dd", "if=/dev/zero", "of=%s" % dev_path, "bs=1M",
                   "count=100", "oflag=direct"]
            call(dbg, cmd)
            cmd = ["blkdiscard", "-v", dev_path]
            (stdout, stderr, rcode) = call(dbg, cmd, error=False, simple=False)
            log.info("%s completed with rc %d: %s - %s"
                     % (" ".join(cmd), rcode, stdout, stderr))
        finally:
            unplug_device(dbg, dev)

    def _detach(self, dbg, sr):
        """
        Detach but don't unplug the SR
        """
        callbacks = gfs2.Callbacks()

        # stop GC
        try:
            COWCoalesce.stop_gc(dbg, "gfs2", sr)
        except:
            log.debug("GC already stopped")

        # Remove the host from the database
        mnt_path = urlparse.urlparse(sr).path
        try:
            database = callbacks.get_database(mnt_path)
            with Lock(mnt_path, dlm_monitor.NODE_CLEANUP_LOCK, callbacks):
                with database.write_context():
                    database.remove_host_by_host_id(
                        callbacks.get_current_host())
        except sqlite3.DatabaseError as db_err:
            # If the storage has gone down we won't be able to access
            # the database
            log.error('{}: SR Detach, unable to clear database {}. {}'.
                      format(dbg, sr, db_err))
        finally:
            database.close()

        # Unmount the FS
        gfs2_umount(dbg, mnt_path)

    def detach(self, dbg, sr):
        meta = util.getSRMetadata(dbg, sr)
        sr_uuid = meta['sr_uuid']

        try:
            if not block_device_exists(sr_uuid):
                log.error("%s: SR detach, couldn't find saved block device for"
                          "SR %s" % (dbg, sr_uuid))
                raise util.create_storage_error("SR_BACKEND_FAILURE_19",
                                                ["SR block device not found"])

            self._detach(dbg, sr)
            block_device = retrieve_block_device(dbg, sr_uuid, cleanup=True)

            # Unplug device if need be
            unplug_device(dbg, block_device)
        except:
            log.error("Error detaching SR %s" % sr)
            raise

    def ls(self, dbg, sr):
        try:
            # refresh transport connection to reflect LUN's new size
            # TODO: Call transport for this
            pass
        except Exception, e:
            log.debug("Exception in SR.ls: %s" % str(e))

        finally:
            return COWVolume.ls(dbg, sr, gfs2.Callbacks())

    def set_description(self, dbg, sr, new_description):
        util.updateSRMetadata(dbg, sr, {'description': new_description})
        return None

    def set_name(self, dbg, sr, new_name):
        util.updateSRMetadata(dbg, sr, {'name': new_name})
        return None

    def stat(self, dbg, sr):
        # SR path (sr) is file://<mnt_path>
        # Get mnt_path by dropping url scheme
        uri = urlparse.urlparse(sr)
        mnt_path = "/%s/%s" % (uri.netloc, uri.path)

        if not(os.path.isdir(mnt_path)) or not(os.path.ismount(mnt_path)):
            raise xapi.storage.api.v4.volume.Sr_not_attached(mnt_path)

        # Get the filesystem size
        statvfs = os.statvfs(mnt_path)
        psize = statvfs.f_blocks * statvfs.f_frsize
        fsize = statvfs.f_bfree * statvfs.f_frsize
        log.debug("%s: statvfs says psize = %Ld" % (dbg, psize))

        overprovision = (
            COWVolume.get_sr_provisioned_size(sr, gfs2.Callbacks()) / psize)

        meta = util.getSRMetadata(dbg, sr)

        return {
            "sr": sr,
            "name": meta["name"],
            "description": meta["description"],
            "total_space": psize,
            "free_space": fsize,
            "uuid": meta["sr_uuid"],
            "overprovision": overprovision,
            "datasources": [],
            "clustered": True,
            "health": ["Healthy", ""]
        }


if __name__ == "__main__":
    log.log_call_argv()
    SR_CMD = xapi.storage.api.v4.volume.SR_commandline(Implementation())
    CMD_BASE = os.path.basename(sys.argv[0])
    if CMD_BASE == 'SR.probe':
        SR_CMD.probe()
    elif CMD_BASE == 'SR.attach':
        SR_CMD.attach()
    elif CMD_BASE == 'SR.create':
        SR_CMD.create()
    elif CMD_BASE == 'SR.destroy':
        SR_CMD.destroy()
    elif CMD_BASE == 'SR.detach':
        SR_CMD.detach()
    elif CMD_BASE == 'SR.ls':
        SR_CMD.ls()
    elif CMD_BASE == 'SR.set_description':
        SR_CMD.set_description()
    elif CMD_BASE == 'SR.set_name':
        SR_CMD.set_name()
    elif CMD_BASE == 'SR.stat':
        SR_CMD.stat()
    else:
        raise xapi.storage.api.v4.volume.Unimplemented(CMD_BASE)
