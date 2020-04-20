"""Cassandra - Handles ingest into cassandra"""
import json
import re

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent


CONCURRENCY = 100
DEFAULT_TIMEOUT = 60

LABEL_NORM_PATTERN = re.compile(r'[\W_]+', re.UNICODE)


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

    def _add_normalized_label(self, tag_json):
        """Enrich JSON Tag representation by normalized labels"""
        # Alphanumeric and lowercase only
        d = json.loads(tag_json)
        label = d['label']
        d['label_norm'] = LABEL_NORM_PATTERN.sub('', label).lower()
        d['label_norm_prefix'] = d['label_norm'][:3]
        return json.dumps(d)

    def insert_tagpack(self, tagpack, keyspace, concurrency):
        """Insert a tagpack into a given keyspace"""
        if not self.session:
            raise StorageError("Session not availble. Call connect() first")
        self.session.set_keyspace(keyspace)

        q = f"INSERT INTO tagpack_by_uri JSON '{tagpack.to_json()}'"
        self.execute_query(q, keyspace)

        stmt_1 = self.session.prepare("INSERT INTO tag_by_address JSON ?")
        stmt_2 = self.session.prepare("INSERT INTO tag_by_category JSON ?")
        stmt_3 = self.session.prepare("INSERT INTO tag_by_label JSON ?")

        statements_and_params = []
        for tag in tagpack.tags:
            # prepare statements for table tag_by_address
            params = (tag.to_json(), )
            statements_and_params.append((stmt_1, params))
            # prepare statements for table tag_by_category
            if 'category' in tag.fields.keys():
                statements_and_params.append((stmt_2, params))
            # prepare statements for table tag_by_label
            tag_w_norm_labels = self._add_normalized_label(tag.to_json())
            params = (tag_w_norm_labels, )
            statements_and_params.append((stmt_3, params))

        results = execute_concurrent(self.session, statements_and_params,
                                     concurrency=concurrency,
                                     raise_on_first_error=True)
        for (success, result) in results:
            if not success:
                raise StorageError(
                    f"Error when inserting tagpack {tagpack.filename}")

    def close(self):
        """Closes the cassandra cluster connection"""
        self.cluster.shutdown()