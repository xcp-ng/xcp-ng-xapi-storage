"""
Monitor for DLM nodes fencng and clean up
"""
import re
import sys
import time

from xapi.storage import log
from xapi.storage.libs import util
from xapi.storage.libs.libcow.callbacks import VolumeContext
from xapi.storage.libs.libcow.lock import Lock

NOW_RE = r'^daemon now (\d+)'
FENCE_RE = r'^node (\d+) X add \d+ rem \d+ fail \d+ fence (\d+) at \d+ \d+'

NODE_CLEANUP_LOCK = 'node_cleanup'


def monitor_dlm(uri, callbacks):
    last_update = 0
    cmd = ['dlm_tool', 'status']

    with VolumeContext(callbacks, uri, 'w') as opq:
        database = callbacks.get_database(opq)
        while True:
            try:
                with Lock(opq, NODE_CLEANUP_LOCK, callbacks):
                    stdout = util.call_unlogged('dlm_monitor', cmd)

                    current_update = last_update

                    for line in stdout.splitlines():
                        now_match = re.match(NOW_RE, line)
                        if now_match:
                            current_update = int(now_match.group(1))

                        fence_match = re.match(FENCE_RE, line)
                        if fence_match:
                            fence_time = int(fence_match.group(2))
                            fence_node = fence_match.group(1)
                            if fence_time > last_update:
                                log.info('Node {} fenced at {}'.format(
                                    fence_node, fence_time))
                                log.debug('stdout:\n{}'.format(stdout))
                                with database.write_context():
                                    database.remove_host_by_cluster_node_id(
                                        str(fence_node))
                    last_update = current_update
            except:
                log.error('Exception in dlm_monitor. {}'.format(
                    sys.exc_info()))

            time.sleep(10)
