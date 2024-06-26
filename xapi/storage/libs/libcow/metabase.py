"""
Metadata database for virtual disks
"""

import logging
import sqlite3
from xapi.storage import log
from xapi.storage.libs import util

class VDI(object):
    """
    Virtual Disk Image (VDI) database convenience class
    """

    def __init__(
            self, uuid, name, description, active_on,
            nonpersistent, volume, sharable):
        self.uuid = uuid
        self.name = name
        self.description = description
        self.active_on = active_on
        self.nonpersistent = nonpersistent
        self.volume = volume
        self.sharable = sharable

    @property
    def image_type(self):
        return self.volume.image_type

    @classmethod
    def from_row(cls, row):
        """
        Construct object from database row
        """
        volume = Volume.from_row(row)
        return cls(
            row['uuid'],
            row['name'],
            row['description'],
            row['active_on'],
            row['nonpersistent'],
            volume,
            row['sharable']
        )


class Volume(object):
    """
    Volume disk object
    """

    def __init__(self, volume_id, parent, snap, vsize, psize, image_type):
        self.id = volume_id
        self.parent_id = parent
        self.snap = snap
        self.vsize = vsize
        self.psize = psize
        self.image_type = image_type

    @classmethod
    def from_row(cls, row):
        """
        Construct object from database row
        """
        return cls(
            row['id'],
            row['parent_id'],
            row['snap'],
            row['vsize'],
            row['psize'],
            row['image_type']
        )


class Journal(object):
    """
    Database convenience object to track node reparenting for crash recovery
    """

    def __init__(self, id, parent_id, new_parent_id):
        self.id = id
        self.parent_id = parent_id
        self.new_parent_id = new_parent_id

    @classmethod
    def from_row(cls, row):
        """
        Construct object from database row
        """
        return cls(
            row['id'],
            row['parent_id'],
            row['new_parent_id']
        )


class Refresh(object):
    """
    Database convenience object to track active nodes requiring a refresh
    """

    def __init__(self, updated_node, leaf_id, new_parent, old_parent):
        self._leaf_id = leaf_id
        self._updated_node = updated_node
        self._new_parent = new_parent
        self._old_parent = old_parent

    @property
    def leaf_id(self):
        """
        The active leaf requiring a tree reload
        """
        return self._leaf_id

    @property
    def updated_node(self):
        """
        The node with updated parentage
        """
        return self._updated_node

    @property
    def new_parent(self):
        """
        The new parent for the updated node
        """
        return self._new_parent

    @property
    def old_parent(self):
        """
        The old, now replaced, parent of the updated node
        """
        return self._old_parent

    @classmethod
    def from_row(cls, row):
        """
        Construct object from database row
        """
        return cls(
            row['child_id'],
            row['leaf_id'],
            row['new_parent_id'],
            row['old_parent_id']
        )

    def __str__(self):
        return '{}: {}'.format(self.updated_node, self.leaf_id)


class VolumeMetabase(object):
    """
    Metadata database
    """

    def __init__(self, path):
        self.__path = path
        self.__connect()

    def __connect(self):
        self._conn = sqlite3.connect(
            self.__path,
            timeout=3600,
            isolation_level='DEFERRED'
        )

        self._conn.execute('PRAGMA foreign_keys = 1')

        self._conn.row_factory = sqlite3.Row

    def _table_exists(self, name):
        return self._conn.execute("""
            SELECT count(*) from sqlite_master
            WHERE type = 'table'
            AND name = '{}'
        """.format(name)).fetchone()[0] == 1

    def _get_version(self, module_name):
        version = 0
        with self._conn:
            if self._table_exists("db_version"):
                ret = self._conn.execute("""
                    SELECT version
                    FROM db_version
                    WHERE module_name = '{}'
                """.format(module_name)).fetchone()
                if ret is not None:
                    version = ret[0]
        return version

    def _set_version(self, module_name, version):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS db_version(
                module_name TEXT PRIMARY KEY NOT NULL,
                version     INTEGER UNSIGNED NOT NULL
            )""")
        self._conn.execute("""
            INSERT OR IGNORE INTO db_version(module_name, version)
            VALUES ('{}', 0)
            """.format(module_name))
        self._conn.execute("""
            UPDATE db_version
            SET version = {}
            WHERE module_name = '{}'
            """.format(version, module_name))

    def _create_tables(self):
        version = self._get_version("volume")
        if version == 0:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS volume(
                    id          INTEGER PRIMARY KEY NOT NULL,
                    snap        BOOLEAN NOT NULL CHECK (snap IN (0, 1)),
                    parent_id   INTEGER,
                    vsize       INTEGER,
                    psize       INTEGER,
                    image_type  INTEGER NOT NULL
                )""")
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS volume_parent ON volume(parent_id)"
            )
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS vdi(
                    uuid          TEXT PRIMARY KEY NOT NULL,
                    name          TEXT,
                    description   TEXT,
                    active_on     TEXT,
                    nonpersistent INTEGER,
                    volume_id     INTEGER NOT NULL UNIQUE,
                    sharable      INTEGER,
                    FOREIGN KEY(volume_id) REFERENCES volume(id)
                )""")
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS vdi_uuid ON vdi(uuid)")
            self._conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS vdi_volume_id ON
                vdi(volume_id)
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS journal(
                    id            INTEGER NOT NULL,
                    parent_id     INTEGER NOT NULL,
                    new_parent_id INTEGER NOT NULL,
                    FOREIGN KEY(id) REFERENCES volume(id),
                    FOREIGN KEY(parent_id) REFERENCES volume(id),
                    FOREIGN KEY(new_parent_id) REFERENCES volume(id)
                 )""")
            self._conn.execute("""
                 CREATE TABLE IF NOT EXISTS refresh(
                     child_id      INTEGER NOT NULL,
                     new_parent_id INTEGER NOT NULL,
                     old_parent_id INTEGER NOT NULL,
                     leaf_id       TEXT NOT NULL,
                     active_on     TEXT NOT NULL,
                     FOREIGN KEY(child_id) REFERENCES volume(id),
                     FOREIGN KEY(new_parent_id) REFERENCES volume(id),
                     FOREIGN KEY(old_parent_id) REFERENCES volume(id),
                     FOREIGN KEY(leaf_id) REFERENCES vdi(uuid)
                 )""")
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS vdi_custom_keys(
                    vdi_uuid       TEXT NOT NULL,
                    key            TEXT NOT NULL,
                    value          TEXT,
                    FOREIGN KEY(vdi_uuid) REFERENCES vdi(uuid),
                    UNIQUE (vdi_uuid, key) ON CONFLICT REPLACE
                )""")
        if version < 1:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS configuration(
                    key    TEXT PRIMARY KEY NOT NULL,
                    value  NOT NULL
                )""")
            self._conn.execute("""
                INSERT OR IGNORE INTO configuration(key, value)
                VALUES ('backup_interval', 3600),
                       ('last_backup_time', 0),
                       ('max_backups', 8)
                """)
            self._set_version("volume", 1)

    def create(self):
        """
        Populate database with tables and indexes
        """
        with self._conn:
            self._create_tables()

    def _set_configuration_property(self, key, value):
        self._conn.execute("""
            UPDATE configuration
            SET value = '{}'
            WHERE key = '{}'
        """.format(value, key))

    def _get_configuration_property(self, key):
        return self._conn.execute("""
            SELECT value
            FROM configuration
            WHERE key = '{}'
         """.format(key)).fetchone()[0]

    @property
    def backup_interval(self):
        return int(self._get_configuration_property("backup_interval"))

    @backup_interval.setter
    def backup_interval(self, backup_interval):
        self._set_configuration_property(
            "backup_interval",
            int(backup_interval)
        )

    @property
    def last_backup_time(self):
        return float(self._get_configuration_property("last_backup_time"))

    @last_backup_time.setter
    def last_backup_time(self, last_backup_time):
        self._set_configuration_property(
            "last_backup_time",
            float(last_backup_time)
        )

    @property
    def max_backups(self):
        return int(self._get_configuration_property("max_backups"))

    @max_backups.setter
    def max_backups(self, max_backups):
        self._set_configuration_property("max_backups", int(max_backups))

    def dump(self, path):
        with open(path, 'w') as file:
            try:
                import codecs
                encoder = codecs.getincrementalencoder('utf-8')()
                for line in self._conn.iterdump():
                    file.write("{}\n".format(encoder.encode(line)))
            except Exception as e:
                log.error(
                    'Failed to dump the metabase to {}: {}'.format(path, e)
                )
                util.remove_path(path, Force=True)
                raise e

    def insert_vdi(self, name, description, uuid, volume_id, sharable):
        """
        Insert a new VDI into the database
        """
        logging.debug("insert_vdi(uuid=%s, volid=%s)", uuid, volume_id)
        self._conn.execute("""
            INSERT INTO vdi(
                uuid, name, description, volume_id, sharable)
            VALUES (
                :uuid, :name, :description, :volume_id, :sharable)""",
                           {"uuid": uuid,
                            "name": name,
                            "description": description,
                            "volume_id": volume_id,
                            "sharable": sharable})

    def delete_vdi(self, uuid):
        """
        Delete a VDI from the database
        """
        self._conn.execute("""
            DELETE from vdi_custom_keys WHERE vdi_uuid=:vdi_uuid
        """, {"vdi_uuid": uuid})
        self._conn.execute("""
            DELETE FROM vdi WHERE uuid=:uuid
        """, {"uuid": uuid})

    def update_vdi_volume_id(self, uuid, volume_id):
        """
        Update the referenced volume for a VDI
        """
        self.__update_vdi(uuid, "volume_id", volume_id)

    def update_vdi_name(self, uuid, name):
        """
        Update the name of the VDI
        """
        self.__update_vdi(uuid, "name", name)

    def update_vdi_description(self, uuid, description):
        """
        Update VDI description
        """
        self.__update_vdi(uuid, "description", description)

    def update_vdi_active_on(self, uuid, active_on):
        """
        Update which host the VDI is active on
        """
        # If no longer active remove outstanding refresh entries
        if not active_on:
            self.remove_refresh_entry(uuid)
        self.__update_vdi(uuid, "active_on", active_on)

    def update_vdi_nonpersistent(self, uuid, nonpersistent):
        """
        Update whether the VDI is non-persistent
        """
        self.__update_vdi(uuid, "nonpersistent", nonpersistent)

    def __update_vdi(self, uuid, key, value):
        self._conn.execute("""
            UPDATE vdi
               SET {} = :{}
             WHERE uuid = :uuid""".format(key, key),
                           {key: value,
                            "uuid": uuid})

    def insert_new_volume(self, vsize, image_type):
        """
        Add a new volume record
        """
        return self.__insert_volume(None, None, vsize, None, image_type)

    def insert_child_volume(self, parent_id, vsize, is_snapshot=False):
        """
        Add a new volume as a child of an existing one
        """
        parent = self.get_volume_by_id(parent_id)
        return self.__insert_volume(
            parent_id, is_snapshot, vsize, None, parent.image_type)

    def delete_volume(self, volume_id):
        """
        Delete a volume from the database
        """
        self._conn.execute("DELETE FROM volume WHERE id=:volume_id",
                           {"volume_id": volume_id})

    def update_volume_parent(self, volume_id, parent):
        """
        Update the parent of a volume
        """
        self.__update_volume(volume_id, "parent_id", parent)

    def update_volume_vsize(self, volume_id, vsize):
        """
        Update the virtual size of a volume
        """
        self.__update_volume(volume_id, "vsize", vsize)

    def update_volume_psize(self, volume_id, psize):
        """
        Update the pyhsical size of a volume
        """
        self.__update_volume(volume_id, "psize", psize)

    def set_volume_as_snapshot(self, volume_id):
        """
        Sets the volume to be a snapshot
        """
        self.__update_volume(volume_id, "snap", 1)

    def __update_volume(self, volume_id, key, value):
        """
        Update a field in the volume table
        """
        self._conn.execute("""
            UPDATE volume
               SET {} = :{}
             WHERE id = :volume_id""".format(key, key),
                           {key: value,
                            "volume_id": volume_id})

    def __insert_volume(self, parent, is_snapshot, vsize, psize, image_type):
        res = self._conn.execute(
            "INSERT INTO volume(parent_id, snap, vsize, psize, image_type)"
            "VALUES (:parent, :snap, :vsize, :psize, :image_type)",
            {"parent": parent,
             "snap": 1 if is_snapshot else 0,
             "vsize": vsize,
             "psize": psize,
             "image_type": image_type}
        )

        return Volume(
            res.lastrowid, parent, is_snapshot, vsize, psize, image_type)

    def get_vdi_by_id(self, vdi_uuid):
        """
        Get VDI object by uuid
        """
        res = self._conn.execute("""
            SELECT *
              FROM vdi
                   INNER JOIN volume
                   ON vdi.volume_id = volume.id
             WHERE uuid = :uuid""",
                                 {"uuid": vdi_uuid})

        row = res.fetchone()
        if row:
            return VDI.from_row(row)

        return None

    def get_vdi_for_volume(self, volume_id):
        """
        Get VDI object for specified volume (if any)
        """
        res = self._conn.execute("""
            SELECT *
             FROM vdi
                  INNER JOIN volume
                          ON vdi.volume_id = volume.id
            WHERE vdi.volume_id = :volume_id""",
                                 {"volume_id": volume_id})
        row = res.fetchone()
        if row:
            return VDI.from_row(row)

        return None

    def get_all_vdis(self):
        """
        Get all VDIs
        """
        res = self._conn.execute("""
            SELECT *
              FROM vdi
                   INNER JOIN volume
                   ON vdi.volume_id = volume.id
        """)

        vdis = []
        for row in res:
            vdis.append(VDI.from_row(row))

        return vdis

    def get_all_volumes(self):
        """
        Get all volumes.
        """
        res = self._conn.execute("SELECT * FROM volume")

        volumes = []
        for row in res:
            volumes.append(Volume.from_row(row))

        return volumes

    def get_children(self, volume_id):
        """
        Get direct children of the specified volume
        """
        res = self._conn.execute(
            "SELECT * FROM volume WHERE parent_id=:parent",
            {"parent": volume_id})
        volumes = []
        for row in res:
            volumes.append(Volume.from_row(row))
        return volumes

    def get_volume_by_id(self, volume_id):
        """
        Get volume object by ID
        """
        res = self._conn.execute("""
            SELECT *
              FROM volume
             WHERE id = :id""",
                                 {"id": volume_id})

        row = res.fetchone()
        if row:
            return Volume.from_row(row)

        return None

    def get_non_leaf_total_psize(self):
        """Returns the total psize of non-leaf volumes"""
        total_psize = 0

        res = self._conn.execute("""
            SELECT psize
              FROM volume
             WHERE psize NOT NULL
        """)

        for row in res:
            total_psize += row['psize']

        return total_psize

    def get_leaf_total_vsize(self):
        """Returns the total vsize of the non-snapshot leaves"""
        res = self._conn.execute("""
            SELECT SUM(vsize) as sum
              FROM volume
             WHERE id NOT IN
                (SELECT parent_id
                   FROM volume
                  WHERE parent_id NOT NULL)
             AND snap = 0
        """)
        row = res.fetchone()
        if row and row['sum']:
            return int(row['sum'])
        return 0

    def find_non_leaf_coalesceable(self):
        """
        Find all non-leaf coalescable volume nodes.

        To be considered the node should be the only child of its parent and
        have children itself.
        """
        res = self._conn.execute("""
            SELECT * FROM
                   (SELECT *, COUNT(id) AS num
                      FROM volume
                     WHERE parent_id NOT NULL
                  GROUP BY parent_id
            ) AS node
             WHERE node.num = 1
               AND node.id IN
                   (SELECT parent_id
                      FROM volume
                     WHERE parent_id NOT NULL
                  GROUP BY parent_id)""")
        volumes = []
        for row in res:
            volumes.append(Volume.from_row(row))
        return volumes

    def find_leaf_coalesceable(self, active_on):
        """
        Find all leaf coalescable volume nodes.

        To be considered the node should be the only child of its parent and
        have no children itself and be either inactive or active on the
        specified host.
        """
        res = self._conn.execute("""
            SELECT *
            FROM
              -- Select nodes with only one leaf child
              (SELECT *
               FROM
                 -- Count nodes with this as parent
                 (SELECT *,
                         COUNT(volume.id) AS num
                  FROM volume
                  WHERE parent_id NOT NULL
                  GROUP BY parent_id ) AS node
               WHERE node.num = 1
                 AND (node.id NOT IN
                        (SELECT parent_id
                         FROM volume
                         WHERE parent_id NOT NULL
                         GROUP BY parent_id))) AS node1
            INNER JOIN vdi ON volume_id=id
            WHERE active_on=:active
              OR active_on IS NULL
        """, {'active': active_on})

        volumes = []
        for row in res:
            volumes.append(Volume.from_row(row))
        return volumes

    def get_garbage_volumes(self):
        """ A garbage volume is a leaf volume with no associated VDI """
        res = self._conn.execute("""
            SELECT * FROM VOLUME
             WHERE id NOT IN
                (SELECT parent_id
                   FROM volume
                  WHERE parent_id NOT NULL
               GROUP BY parent_id)
                AND id NOT IN
                 (SELECT volume_id
                    FROM vdi
                GROUP BY volume_id)
                AND id NOT IN
                 (SELECT old_parent_id
                    FROM refresh)""")

        volumes = []
        for row in res:
            volumes.append(Volume.from_row(row))

        return volumes

    def add_journal_entries(self, parent_id, new_parent_id, children):
        """ Add journal entries for post-coalesce reparenting.

        Keyword arguments:
        parent_id     -- the current parent of the children
        new_parent_id -- the new parent for the children
        children      -- list of volume objects to be re-parented
        Keyword return:
        A list of Journal objects, one for each child in children
        """
        entries = []
        for child in children:
            self._conn.execute("""
                INSERT INTO journal(id, parent_id, new_parent_id)
                VALUES(:id, :parent_id, :new_parent_id)""",
                               {"id": child.id,
                                "parent_id": parent_id,
                                "new_parent_id": new_parent_id})
            entries.append(Journal(child.id, parent_id, new_parent_id))

        return entries

    def get_journal_entries(self):
        """
        Get all journal entries
        """
        res = self._conn.execute("SELECT * from journal")

        journal_entries = []
        for row in res:
            journal_entries.append(Journal.from_row(row))

        return journal_entries

    def remove_journal_entry(self, entry_id):
        """
        Remove the specified journal entry
        """
        self._conn.execute("""
            DELETE FROM journal WHERE id=:id""",
                           {"id": entry_id})

    def add_refresh_entries(
            self, volume_id, old_parent_id, new_parent_id, leaves):
        """ Add refresh entries for post-reparenting refresh

        Keyword arguments:
        volume_id -- the volume that has been reparented
        leaves -- the leaves that need to be refreshed
        Keyword return:
        A list of Refresh  objects, one for each leaf in leaves
        """
        entries = []
        for leaf in leaves:
            self._conn.execute("""
                INSERT INTO refresh(child_id, old_parent_id, new_parent_id,
                                    leaf_id, active_on)
                VALUES(:child_id, :old_parent_id, :new_parent_id,
                       :leaf_id, :active_on)""",
                               {"child_id": volume_id,
                                "old_parent_id": old_parent_id,
                                "new_parent_id": new_parent_id,
                                "leaf_id": leaf.uuid,
                                "active_on": leaf.active_on})
            entries.append(Refresh(
                volume_id, leaf.uuid, new_parent_id, old_parent_id))
        return entries

    def get_refresh_entries(self, active_on):
        """
        Get all entries in the refresh table
        """
        res = self._conn.execute(
            "SELECT * FROM refresh WHERE active_on=:active_on",
            {"active_on": active_on})
        refresh_entries = []
        for row in res:
            refresh_entries.append(Refresh.from_row(row))
        return refresh_entries

    def remove_refresh_entry(self, leaf_id):
        """
        Remove the refresh entry for the specified leaf id
        """
        self._conn.execute("""
            DELETE FROM refresh WHERE leaf_id=:leaf_id""",
                           {'leaf_id': leaf_id})

    def get_vdi_custom_keys(self, vdi_uuid):
        """
        Get all custom_keys for vdi_uuid
        """
        res = self._conn.execute("""
             SELECT * FROM vdi_custom_keys
             WHERE vdi_uuid=:vdi_uuid""", {"vdi_uuid": vdi_uuid})

        custom_keys = {}
        for row in res:
            custom_keys[str(row["key"])] = row["value"]
        return custom_keys

    def get_all_vdi_custom_keys(self):
        """
        Get all VDI custom keys
        """
        res = self._conn.execute("SELECT * FROM vdi_custom_keys")
        custom_keys = {}
        for row in res:
            if row["vdi_uuid"] not in custom_keys:
                custom_keys[row["vdi_uuid"]] = {}
            custom_keys[row["vdi_uuid"]][row["key"]] = row["value"]
        return custom_keys

    def set_vdi_custom_key(self, vdi_uuid, custom_key, value):
        """
        Update custom_key with value for vdi_uuid
        """
        self._conn.execute("""
             INSERT OR REPLACE INTO vdi_custom_keys(vdi_uuid, key, value)
             VALUES(:vdi_uuid, :key, :value)""",
                           {"vdi_uuid": vdi_uuid,
                            "key": custom_key,
                            "value": value})

    def delete_vdi_custom_key(self, vdi_uuid, custom_key):
        """
        Delete the specified custom_key for the specified vdi_uuid
        """
        self._conn.execute("""
            DELETE from vdi_custom_keys WHERE vdi_uuid=:vdi_uuid
            AND key=:key""",
                           {"vdi_uuid": vdi_uuid,
                            "key": custom_key})

    def get_vdi_chain_height(self, vdi_uuid):
        """
        Return the height of the volume tree for a given VDI
        """
        volume_count = 0
        vdi = self.get_vdi_by_id(vdi_uuid)
        volume = vdi.volume

        while volume:
            volume_count += 1

            volume = self.get_volume_by_id(volume.parent_id)

        return volume_count

    def clear_host_references(self, host_id):
        """
        Removes the links to hosts in VDI and refresh tables
        """
        self._conn.execute("""
            UPDATE vdi
               SET active_on = NULL
             WHERE active_on = :host_id""",
                           {'host_id': host_id})
        self._conn.execute("""
            DELETE FROM refresh
            WHERE active_on = :host_id""",
                           {'host_id': host_id})

    def close(self):
        """
        Close the connection to the database
        """
        self._conn.close()
