#!/usr/bin/python

import time
import subprocess
from xapi.storage.libs import qmp
from xapi.storage.libs.util import var_run_prefix
import sys


def watch(path):
    cmd = ["/usr/bin/xenstore-watch", path]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    proc.stdout.readline()  # discard spurious watch event
    return proc


def read(path):
    cmd = ["/usr/bin/xenstore-read", path]
    data, _ = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()
    return data.strip()


def found_new_device(domid, devid, uuid, type):
    q = qmp.QEMUMonitorProtocol(
        '{}/qemu-dp/qmp_sock.{}'.format(var_run_prefix(), uuid))

    path = '{}/{}'.format(var_run_prefix(), uuid)
    with open(path, 'w') as f:
        f.write("/local/domain/{}/device/vbd/{}/state".format(domid, devid))

    params = {}
    params['domid'] = domid
    params['devid'] = devid
    params['type'] = type
    params['blocknode'] = 'qemu_node'
    params['devicename'] = uuid

    connected = False
    count = 0
    while not connected:
        try:
            q.connect()
            connected = True
        except:
            if count > 5:
                print "ERROR: not delivering xen-watch-device %s" % params
                return
            print ("got exception {};"
                   " sleeping before attempting reconnect...".format(
                       sys.exc_info()))
            time.sleep(1)
            count += 1

    print "calling: xen-watch-device %s" % params
    res = q.command('xen-watch-device', **params)
    print "result: %s" % res


proc = watch("/local/domain/0/backend")

while True:
    path = proc.stdout.readline().strip()  # block until we get an event
    tokens = path.split('/')

    if len(tokens) <= 8:
        continue

    type = tokens[5]
    if (type == 'qdisk' or type == '9pfs') and tokens[8] == 'qemu-params':
        domid = int(tokens[6])
        devid = int(tokens[7])
        contents = read(path)
        print ("Found new device with domid=%d devid=%d type=%s contents=%s"
               % (domid, devid, type, contents))

        # "contents" values:
        # - if type == qdsik: "vdi:<VDI_UUID>"
        # - if type == 9pfs: "vdi:<VDI_UUID> <9PFS_TAG> <9PFS_SECURITY_MODEL> <9PFS_PATH>"
        (prefix, uuid) = contents.split(' ')[0].split(':')
        if prefix == 'vdi':
            found_new_device(domid, devid, uuid, type)
