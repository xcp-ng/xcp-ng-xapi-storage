"""
SR database for GFS2
"""

from xapi.storage.libs.libcow.metabase import VolumeMetabase


class Host(object):
    """
    Host object for managing lun size and liveness
    """
    def __init__(self, host_id, cluster_node_id, lun_size):
        self._host_id = host_id
        self._cluster_node_id = cluster_node_id
        self._lun_size = lun_size

    @property
    def host_id(self):
        """
        Host installation unique identifier
        """
        return self._host_id

    @property
    def cluster_node_id(self):
        """
        Corosync cluster node id for this host
        """
        return self._cluster_node_id

    @property
    def lun_size(self):
        """
        Size of the SR LUN seen by this host
        """
        return self._lun_size

    @classmethod
    def from_row(cls, row):
        """
        Construct a host object from database row
        """
        return cls(
            row['host_id'],
            row['cluster_node_id'],
            row['lun_size'])


class GFS2Database(VolumeMetabase):
    """
    GFS2 SR specific database extensions
    """

    def create(self):
        """
        Create the database with additonal GFS2 tables
        """
        super(GFS2Database, self).create()

        with self._conn:
            self._conn.execute("""
                CREATE TABLE host(
                    host_id          TEXT PRIMARY KEY NOT NULL,
                    cluster_node_id  TEXT NOT NULL,
                    lun_size         INTEGER NOT NULL
                )""")

    def add_host(self, host_id, cluster_node_id, lun_size):
        """
        Add a host to the database
        """
        self._conn.execute("""
            INSERT INTO host(
                host_id, cluster_node_id, lun_size)
            VALUES (
                :host_id, :cluster_node_id, :lun_size)""",
                           {'host_id': host_id,
                            'cluster_node_id': cluster_node_id,
                            'lun_size': lun_size})

    def get_host_by_host_id(self, host_id):
        """
        Retrieve a host from the database by host id
        """
        res = self._conn.execute(
            "SELECT * FROM host WHERE host_id = :host_id",
            {'host_id': host_id})

        row = res.fetchone()
        if row:
            return Host.from_row(row)

        return None

    def get_host_by_cluster_node_id(self, cluster_node_id):
        """
        Retrieve a host from the database by cluster node id
        """
        res = self._conn.execute(
            "SELECT * FROM host WHERE cluster_node_id = :cluster_node_id",
            {'cluster_node_id': cluster_node_id})

        row = res.fetchone()
        if row:
            return Host.from_row(row)

        return None

    def remove_host_by_host_id(self, host_id):
        """
        Remove a host with a given host_id
        """
        self._conn.execute(
            "DELETE FROM host WHERE host_id = :host_id",
            {'host_id': host_id})
        self.clear_host_references(host_id)

    def remove_host_by_cluster_node_id(self, cluster_node_id):
        """
        Remove a host with a given cluster node id
        """
        host = self.get_host_by_cluster_node_id(cluster_node_id)
        if host:
            self.remove_host_by_host_id(host.host_id)
