"""Cassandra - Handles ingest into cassandra"""
from cassandra.cluster import Cluster


BATCH_SIZE_LIMIT = 500
DEFAULT_TIMEOUT = 60


class StorageError(Exception):
    """Class for Cassandra-related errors"""

    def __init__(self, message, nested_exception=None):
        super().__init__("Cassandra Error: " + message)
        self.nested_exception = nested_exception

    def __str__(self):
        msg = super(StorageError, self).__str__()
        if self.nested_exception:
            msg = msg + '\nError Details: ' + str(self.nested_exception)
        return msg


class Cassandra(object):
    """Cassandra Backend Connector

    Taxonomies and TagPacks can be ingested into Apache Cassandra#
    for futher processing within GraphSense.

    This class provides the necessary schema creation and data
    ingesting functions.

    """

    def __init__(self, db_nodes):
        self.db_nodes = db_nodes

    def connect(self):
        """Connect to the given Cassandra cluster nodes"""
        self.cluster = Cluster(self.db_nodes)
        try:
            self.session = self.cluster.connect()
            self.session.default_timeout = DEFAULT_TIMEOUT
        except Exception as e:
            raise StorageError("Cannot connect to {}".format(self.db_nodes), e)

    def execute_query(self, query, keyspace=None):
        """Execute a query against a given keyspace"""
        if not self.session:
            raise StorageError("Session not availble. Call connect() first")
        try:
            if keyspace:
                self.session.set_keyspace(keyspace)
            result = self.session.execute(query)
            return result
        except Exception as e:
            raise StorageError("Error when executing {}".format(query), e)

    def has_keyspace(self, keyspace):
        """Check whether a given keyspace is present in the cluster"""
        query = 'SELECT keyspace_name FROM system_schema.keyspaces'
        result = self.execute_query(query)
        keyspaces = [row.keyspace_name for row in result]
        return keyspace in keyspaces

    def insert_taxonomy(self, taxonomy, keyspace):
        """Insert a taxonomy into a given keyspace"""
        query = "INSERT INTO taxonomy_by_key JSON '{}';".format(
            taxonomy.to_json())
        self.execute_query(query, keyspace)
        for concept in taxonomy.concepts:
            concept_json = concept.to_json()
            query = "INSERT INTO concept_by_taxonomy_id JSON '{}';".format(
                concept_json.replace("'", ""))
            self.execute_query(query, keyspace)
            print("Inserted concept {}".format(concept.id))

    def insert_tagpack(self, tagpack, keyspace):
        """Insert a tagpack into a given keyspace"""
        if not self.session:
            raise StorageError("Session not availble. Call connect() first")
        self.session.set_keyspace(keyspace)

        try:
            q = f"INSERT INTO tagpack_by_uri JSON '{tagpack.to_json()}'"
            self.execute_query(q, keyspace)

            for tag in tagpack.tags:
                self._insert_tag_by_address(tag)

        except Exception as e:
            raise StorageError(f"Error when inserting tagpack {e}", e)

    def _insert_tag_by_address(self, tag):
        try:
            stmt = self.session.prepare("INSERT INTO tag_by_address JSON ?")
            self.session.execute(stmt, [tag.to_json()])
        except Exception as e:
            raise StorageError(f"Error when inserting tag {tag}", e)

    def close(self):
        """Closes the cassandra cluster connection"""
        self.cluster.shutdown()
        print("Closed Cassandra cluster connection")
