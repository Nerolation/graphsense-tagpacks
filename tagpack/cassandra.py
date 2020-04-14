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
        print("Connecting to Cassandra cluster @ {}".format(self.db_nodes))
        self.cluster = Cluster(self.db_nodes)
        try:
            self.session = self.cluster.connect()
            self.session.default_timeout = DEFAULT_TIMEOUT
        except Exception as e:
            raise StorageError("Cannot connect to {}".format(self.db_nodes), e)

    def execute_query(self, query, keyspace=None):
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
        query = 'SELECT keyspace_name FROM system_schema.keyspaces'
        result = self.execute_query(query)
        keyspaces = [row.keyspace_name for row in result]
        return keyspace in keyspaces

    def insert_taxonomy(self, taxonomy, keyspace):
        print("Inserting taxonomy {} into {} ".format(taxonomy, keyspace))
        for concept in taxonomy.concepts:
            concept_json = concept.to_json()
            query = """INSERT INTO concept JSON '{}';""".format(
                concept_json.replace("'", ""))
            self.execute_query(query, keyspace)
            print("Inserted concept {}".format(concept.id))

    def close(self):
        self.cluster.shutdown()
        print("Closed Cassandra cluster connection")
