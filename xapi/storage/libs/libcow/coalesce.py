#!/usr/bin/env python
"""
Garbage collector and tree coalesce
"""

from __future__ import absolute_import
import os
import pickle
import re
import subprocess
import sys
import time

from xapi.storage import log
from xapi.storage.libs import util

from xapi.storage.libs.libcow.callbacks import VolumeContext
from xapi.storage.libs.libcow.imageformat import ImageFormat
from xapi.storage.libs.libcow.lock import PollLock


# Debug string
GC = 'GC'

_MIB = 2**20
_LEAF_COALESCE_MAX_SIZE = 20 * _MIB

PRIO_GC = 1


class VolumeLock(object):
    """
    Define a grouping of a volume file and the lock for it
    """

    def __init__(self, volume, lock):
        self.volume = volume
        self.lock = lock


def __refresh_leaf_vdis(opq, callbacks, leaves):
    """
    Refresh the supplied leaf VDIs to reload the delta tree
    """
    for leaf in leaves:
        with callbacks.db_context(opq) as db:
            vdi = db.get_vdi_by_id(leaf.leaf_id)
        log.debug('Refreshing datapath for {}'.format(vdi.uuid))
        refresh_live_cow_chain(vdi, leaf, callbacks, opq)
        with callbacks.db_context(opq) as db:
            db.remove_refresh_entry(vdi.uuid)


def __reparent_children(opq, callbacks, journal_entries):
    """
    Reparent the children of a node after it has been coalesced
    """
    for child in journal_entries:
        child_path = callbacks.volumeGetPath(opq, str(child.id))
        with callbacks.db_context(opq) as db:
            child_volume = db.get_volume_by_id(child.id)

            # Find all leaves having child as an ancestor
            leaves = []
            log.debug('Find active leaves of {}'.format(child.id))
            find_active_leaves(child_volume, db, leaves)

        image_utils = ImageFormat.get_format(
            child_volume.image_type).image_utils

        # reparent child to grandparent
        log.debug("Reparenting {} to {}".format(child.id, child.new_parent_id))
        with callbacks.db_context(opq) as db:
            db.update_volume_parent(child.id, child.new_parent_id)
            new_parent_path = callbacks.volumeGetPath(
                opq, str(child.new_parent_id))
            image_utils.set_parent(GC, child_path, new_parent_path)
            db.remove_journal_entry(child.id)
            # Add leaves to database
            if leaves:
                # Refresh all leaves having child as an ancestor
                leaves_to_refresh = db.add_refresh_entries(
                    child.id, child.parent_id, child.new_parent_id, leaves)
                log.debug("Children {}: leaves: {} will be refreshed".format(
                    child.id, [str(x) for x in leaves_to_refresh]))


def __find_non_leaf_coalesceable(database):
    """
    Find the non leaf coalescable nodes from the database
    """
    results = database.find_non_leaf_coalesceable()
    if len(results) > 0:
        log.debug("Found {} non leaf coalescable nodes".format(len(results)))
    return results


def __find_leaf_coalesceable(this_host, database):
    """
    Find all the leaf coalesable nodes from the database
    """
    results = database.find_leaf_coalesceable(this_host)
    if results:
        log.debug("Found {} leaf coalescable nodes".format(len(results)))
    return results


def _find_best_leaf_coalesceable(this_host, uri, callbacks):
    """
    Find the next pair of COW nodes to be leaf coalesced
    """
    with VolumeContext(callbacks, uri, 'w') as opq:
        with PollLock(opq, 'gl', callbacks, PRIO_GC):
            with callbacks.db_context(opq) as db:
                nodes = __find_leaf_coalesceable(this_host, db)
            for node in nodes:
                # Temp: no leaf on qcow2 for now
                # Support of QCOW2 has been disabled, comment the code until we
                # reenable it...
                #if node.image_type == ImageFormat.IMAGE_QCOW2:
                #    continue
                with callbacks.db_context(opq) as db:
                    leaf, parent = __lock_node_pair(node, opq, db, callbacks)
                if (leaf, parent) != (None, None):
                    __leaf_coalesce(leaf, parent, opq, callbacks)
                    return True
    return False


def find_active_leaves(volume, database, leaf_accumulator):
    """
    Recursively find the active leaf nodes of the specified volume
    """
    if not volume:
        return

    children = database.get_children(volume.id)
    if len(children) == 0:
        # This is a leaf add it to list
        vdi = database.get_vdi_for_volume(volume.id)
        if vdi and vdi.active_on:
            leaf_accumulator.append(vdi)
    else:
        for child in children:
            find_active_leaves(child, database, leaf_accumulator)


def refresh_live_cow_chain(vdi, refresh, callbacks, opq):
    """
    Refresh the datapath for the vdi
    """
    assert vdi.active_on
    vdi_meta_path = callbacks.get_data_metadata_path(opq, vdi.uuid)
    image_utils = ImageFormat.get_format(vdi.image_type).image_utils
    image_utils.refresh_datapath_coalesce(
        GC, vdi_meta_path,
        callbacks.volumeGetPath(opq, str(refresh.old_parent)),
        callbacks.volumeGetPath(opq, str(refresh.new_parent)))


def __leaf_coalesce(leaf, parent, opq, callbacks):
    """
    Perform leaf volume coalesce.

    Must be called from inside a global SR lock.
    """
    leaf_volume = leaf.volume
    parent_volume = parent.volume

    log.debug(
        'leaf_coalesce key={}, parent={}'.format(
            leaf_volume.id, parent_volume.id)
    )

    leaf_path = callbacks.volumeGetPath(opq, str(leaf_volume.id))
    parent_path = callbacks.volumeGetPath(opq, str(parent_volume.id))
    leaf_psize = os.path.getsize(leaf_path)

    try:
        with callbacks.db_context(opq) as db:
            vdi = db.get_vdi_for_volume(leaf_volume.id)
        image_utils = ImageFormat.get_format(vdi.image_type).image_utils

        vdi_meta_path = callbacks.get_data_metadata_path(opq, vdi.uuid)

        if leaf_psize < _LEAF_COALESCE_MAX_SIZE:
            log.debug("Running leaf-coalesce on {}".format(leaf_volume.id))

            image_utils.coalesce(GC, leaf_path, parent_path)

            if vdi.active_on:
                image_utils.pause_datapath(GC, vdi_meta_path)

            with callbacks.db_context(opq) as db:
                db.update_vdi_volume_id(vdi.uuid, leaf_volume.parent_id)

            if vdi.active_on:
                image_utils.unpause_datapath(
                    GC, vdi_meta_path, parent_path)

            with callbacks.db_context(opq) as db:
                db.delete_volume(leaf_volume.id)
                callbacks.volumeDestroy(opq, str(leaf_volume.id))
        else:
            # If the leaf is larger than the maximum size allowed for
            # a live leaf coalesce to happen, snapshot it and let
            # non_leaf_coalesce() take care of it.

            log.debug(
                "Snapshot {} and let non-leaf-coalesce handle it".format(
                    leaf_volume.id
                )
            )

            with callbacks.db_context(opq) as db:
                new_leaf_volume = db.insert_child_volume(
                    leaf_volume.id,
                    leaf_volume.vsize
                )

                new_leaf_path = callbacks.volumeCreate(
                    opq,
                    str(new_leaf_volume.id),
                    leaf_volume.vsize
                )

                image_utils.online_snapshot(
                    GC, new_leaf_path, leaf_path, False)

                db.update_vdi_volume_id(vdi.uuid, new_leaf_volume.id)

                if vdi.active_on:
                    image_utils.refresh_datapath_clone(
                        GC, vdi_meta_path, new_leaf_path)

    finally:
        callbacks.volumeUnlock(opq, leaf.lock)
        callbacks.volumeUnlock(opq, parent.lock)


def non_leaf_coalesce(node, parent, uri, callbacks):
    """
    Perform non-leaf (mid tree) volume coalesce
    """
    node_volume = node.volume
    parent_volume = parent.volume

    log.debug("non_leaf_coalesce key={}, parent={}".format(
        node_volume.id, parent_volume.id))

    with VolumeContext(callbacks, uri, 'w') as opq:
        node_path = callbacks.volumeGetPath(opq, str(node_volume.id))
        parent_path = callbacks.volumeGetPath(opq, str(parent_volume.id))
        log.debug("Running cow-coalesce on {}".format(node_volume.id))
        image_utils = ImageFormat.get_format(
            node_volume.image_type).image_utils
        image_utils.coalesce(GC, node_path, parent_path)

        with PollLock(opq, 'gl', callbacks, PRIO_GC):
            with callbacks.db_context(opq) as db:
                # reparent all of the children to this node's parent
                children = db.get_children(node_volume.id)
                journal_entries = db.add_journal_entries(
                    node_volume.id, parent_volume.id, children)

            __reparent_children(opq, callbacks, journal_entries)

            callbacks.volumeUnlock(opq, node.lock)
            callbacks.volumeUnlock(opq, parent.lock)


def __lock_node_pair(node, opq, database, callbacks):
    ret = (None, None)
    parent_lock = callbacks.volumeTryLock(opq, str(node.parent_id))
    if parent_lock:
        node_lock = callbacks.volumeTryLock(opq, str(node.id))
        if node_lock:
            parent = database.get_volume_by_id(node.parent_id)
            ret = (VolumeLock(node, node_lock),
                   VolumeLock(parent, parent_lock))
        else:
            callbacks.volumeUnlock(opq, parent_lock)
    return ret


def _find_best_non_leaf_coalesceable(uri, callbacks):
    """
    Find the next pair of COW nodes to be coalesced
    """
    with VolumeContext(callbacks, uri, 'w') as opq:
        ret = (None, None)
        with PollLock(opq, 'gl', callbacks, PRIO_GC):
            with callbacks.db_context(opq) as db:
                nodes = __find_non_leaf_coalesceable(db)
                for node in nodes:
                    ret = __lock_node_pair(node, opq, db, callbacks)
                    if ret != (None, None):
                        break
    return ret


def recover_journal(uri, this_host, callbacks):
    """
    Complete recover operations started in a different instance
    """
    with VolumeContext(callbacks, uri, 'w') as opq:
        # Take the global SR lock, the coaleasce reparenting happens within
        # this lock, so if we can get it and if there are any pending
        # operations then a different process crashed or was aborted and we
        # need to complete the outstanding operations
        with PollLock(opq, 'gl', callbacks, PRIO_GC):
            with callbacks.db_context(opq) as db:
                # Get the journalled reparent operations
                journal_entries = db.get_journal_entries()
            __reparent_children(opq, callbacks, journal_entries)

            # Now refresh any leaves
            with callbacks.db_context(opq) as db:
                refresh_entries = db.get_refresh_entries(this_host)
            __refresh_leaf_vdis(opq, callbacks, refresh_entries)


def remove_garbage_volumes(uri, callbacks):
    """
    Find any unreferenced, garbage COW nodes and remove
    """
    with VolumeContext(callbacks, uri, 'w') as opq:
        with PollLock(opq, 'gl', callbacks, PRIO_GC):
            with callbacks.db_context(opq) as db:
                garbage = db.get_garbage_volumes()

            if len(garbage) > 0:
                for volume in garbage:
                    with callbacks.db_context(opq) as db:
                        db.delete_volume(volume.id)
                        callbacks.volumeDestroy(opq, str(volume.id))
        callbacks.empty_trash(opq)


def start_task(dbg_msg, uri, callbacks, name, args):
    with VolumeContext(callbacks, uri, 'w') as opq:
        task = subprocess.Popen(args)
        log.debug(dbg_msg)
        path = os.path.join(util.var_run_prefix(), 'sr',
                            callbacks.getUniqueIdentifier(opq))
        try:
            os.makedirs(path)
        except OSError:
            pass
        path = os.path.join(path, name + '_task.pickle')
        with open(path, 'w+') as f:
            pickle.dump(task, f)


def stop_task(dbg_msg, uri, callbacks, name):
    def stop():
        with VolumeContext(callbacks, uri, 'w') as opq:
            path = os.path.join(util.var_run_prefix(), 'sr',
                                callbacks.getUniqueIdentifier(opq),
                                name + '_task.pickle')
            with open(path) as f:
                process = pickle.load(f)
            process.kill()
            process.wait()
            os.unlink(path)

    try:
        with util.timeout(5):
            stop()
    except util.TimeoutException:
        log.error('Timeout reached for task: {}'.format(name))
    log.debug(dbg_msg)


def start_background_tasks(dbg, sr_type, uri, callbacks):
    tasks = callbacks.get_background_tasks()
    if tasks:
        for name, path in tasks:
            args = [path, sr_type, uri]
            dbg_msg = "{}: Starting {} sr_type={} uri={}".format(
                dbg, name, sr_type, uri)
            start_task(dbg_msg, uri, callbacks, name, args)


def gc_is_enabled(uri, callbacks):
    with VolumeContext(callbacks, uri, 'w') as opq:
        return not os.path.exists(os.path.join(
            '/var/lib/sr',
            callbacks.getUniqueIdentifier(opq),
            'gc_disabled'
        ))


def run_coalesce(sr_type, uri):
    """
    GC/Coalesce main loop
    """
    util.daemonize()

    callbacks = util.get_sr_callbacks(sr_type)
    this_host = callbacks.get_current_host()

    while gc_is_enabled(uri, callbacks):
        done_work = False
        try:
            remove_garbage_volumes(uri, callbacks)

            recover_journal(uri, this_host, callbacks)

            child, parent = _find_best_non_leaf_coalesceable(uri, callbacks)
            if (child, parent) != (None, None):
                non_leaf_coalesce(child, parent, uri, callbacks)
                done_work = True
            elif _find_best_leaf_coalesceable(this_host, uri, callbacks):
                done_work = True

            # If we did no work then delay by some time
            if not done_work:
                time.sleep(30)
            else:
                time.sleep(10)

        except Exception:
            import traceback
            log.error("Exception in GC main loop {}, {}".format(
                sys.exc_info(), traceback.format_exc()))
            raise
    log.debug('Stopping GC daemon... Is now disabled')


class COWCoalesce(object):
    """
    Coalescing garbage collector for Copy on Write (COW) nodes
    """
    @staticmethod
    def start_gc(dbg, sr_type, uri):
        # Ensure trash directory exists before starting GC.
        callbacks = util.get_sr_callbacks(sr_type)
        with VolumeContext(callbacks, uri, 'w') as opq:
            callbacks.create_trash_dir(opq)

        if gc_is_enabled(uri, callbacks):
            # Get the command to run, need to replace pyc with py as __file__
            # will be the byte compiled file.
            dbg_msg = "{}: Starting GC sr_type={} uri={}".format(
                dbg, sr_type, uri
            )
            args = [
                os.path.abspath(re.sub("pyc$", "py", __file__)), sr_type, uri
            ]
            start_task(dbg_msg, uri, callbacks, "gc", args)
        else:
            log.debug('GC is disabled, cannot start it')
        start_background_tasks(dbg, sr_type, uri, callbacks)

    @staticmethod
    def stop_gc(dbg, sr_type, uri):
        tasks = [('gc', None)]

        callbacks = util.get_sr_callbacks(sr_type)
        background_tasks = callbacks.get_background_tasks()
        if background_tasks:
            tasks.extend(background_tasks)

        # Stop gc first and then other background tasks.
        for name, _ in tasks:
            log.debug(
                "{}: Attempting to kill {} process for sr_type={}, uri={}"
                .format(dbg, name, sr_type, uri))
            dbg_msg = "{}: {} process killed for uri={}".format(dbg, name, uri)
            stop_task(dbg_msg, uri, callbacks, name)


if __name__ == '__main__':
    try:
        SR_TYPE = sys.argv[1]
        URI = sys.argv[2]
        run_coalesce(SR_TYPE, URI)
    except RuntimeError:
        log.error("libcow:coalesce: error {}", exc_info=True)
        raise
