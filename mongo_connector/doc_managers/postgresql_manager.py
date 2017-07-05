# coding: utf8

import json
import logging
import os.path
import traceback

import psycopg2
from bson.objectid import ObjectId
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.doc_managers.formatters import DocumentFlattener
from mongo_connector.errors import InvalidConfiguration
from psycopg2.extensions import register_adapter
from pymongo import MongoClient

from mongo_connector.doc_managers.mappings import (
    is_mapped,
    get_mapped_document,
    get_primary_key,
    get_scalar_array_fields,
    validate_mapping
)

from mongo_connector.doc_managers.sql import (
    sql_table_exists,
    sql_create_table,
    sql_delete_rows,
    sql_bulk_insert,
    object_id_adapter,
    sql_delete_rows_where,
    to_sql_value,
    sql_drop_table,
    sql_add_foreign_keys
)

from mongo_connector.doc_managers.utils import (
    get_array_fields,
    db_and_collection,
    get_any_array_fields,
    ARRAY_OF_SCALARS_TYPE,
    ARRAY_TYPE,
    get_nested_field_from_document
)


DEFAULT_MAPPINGS_JSON_FILE_NAME = 'mappings.json'

logging.basicConfig()
LOG = logging.getLogger(__name__)


class DocManager(DocManagerBase):
    """DocManager that connects to any SQL database"""

    def insert_file(self, f, namespace, timestamp):
        pass

    def __init__(self, url, unique_key='_id', auto_commit_interval=None, chunk_size=100, **kwargs):
        if 'mongoUrl' not in kwargs:
            raise InvalidConfiguration("The MongoUrl parameter is mandatory.")

        self.url = url
        self.unique_key = unique_key
        self.auto_commit_interval = auto_commit_interval
        self.chunk_size = chunk_size
        self._formatter = DocumentFlattener()
        self.pgsql = psycopg2.connect(url)
        self.insert_accumulator = {}
        self.client = MongoClient(kwargs['mongoUrl'])
        self.quiet = kwargs.get('quiet', False)

        mappings_json_file_name = kwargs.get('mappingFile', DEFAULT_MAPPINGS_JSON_FILE_NAME)
        register_adapter(ObjectId, object_id_adapter)

        if not os.path.isfile(mappings_json_file_name):
            raise InvalidConfiguration("no mapping file found at {}".format(mappings_json_file_name))

        with open(mappings_json_file_name) as mappings_file:
            self.mappings = json.load(mappings_file)

        self.pgsql.set_session(deferrable=True)
        validate_mapping(self.mappings)
        self.pgsql.set_session(deferrable=True)
        self._init_schema()

    def _init_schema(self):
        self.prepare_mappings()

<<<<<<< HEAD
        for database in self.mappings:
            foreign_keys = []

            with self.pgsql.cursor() as cursor:
                for collection in self.mappings[database]:
                    self.insert_accumulator[collection] = 0

                    pk_found = False
                    pk_name = self.mappings[database][collection]['pk']
                    columns = ['_creationdate TIMESTAMP']
                    indices = [u"INDEX idx_{0}__creation_date ON {0} (_creationdate DESC)".format(collection)] + \
                              self.mappings[database][collection].get('indices', [])
=======
        try:
            for database in self.mappings:
                for collection in self.mappings[database]:
                    self.insert_accumulator[collection] = 0

                    with self.pgsql.cursor() as cursor:
                        pk_found = False
                        pk_name = self.mappings[database][collection]['pk']
                        columns = ['_creationdate TIMESTAMP']
                        indices = [u"INDEX idx_{0}__creation_date ON {0} (_creationdate DESC)".format(collection)] + \
                                  self.mappings[database][collection].get('indices', [])
>>>>>>> feature-log-errors

                        for column in self.mappings[database][collection]:
                            column_mapping = self.mappings[database][collection][column]

                            if 'dest' in column_mapping:
                                name = column_mapping['dest']
                                column_type = column_mapping['type']

                                constraints = ''
                                if name == pk_name:
                                    constraints = "CONSTRAINT {0}_PK PRIMARY KEY".format(collection.upper())
                                    pk_found = True

                                if column_type != ARRAY_TYPE and column_type != ARRAY_OF_SCALARS_TYPE:
                                    columns.append(name + ' ' + column_type + ' ' + constraints)

                                if 'index' in column_mapping:
                                    indices.append(u"INDEX idx_{2}_{0} ON {1} ({0})".format(name, collection, collection.replace('.', '_')))

<<<<<<< HEAD
                        if 'fk' in column_mapping:
                            foreign_keys.append({
                                'table': column_mapping['dest'],
                                'ref': collection,
                                'fk': column_mapping['fk'],
                                'pk': pk_name
                            })

                    if not pk_found:
                        constraints = "CONSTRAINT {0}_PK PRIMARY KEY".format(collection.upper())
                        columns.append(pk_name + ' TEXT ' + constraints)
=======
                        if not pk_found:
                            columns.append(pk_name + ' SERIAL CONSTRAINT ' + collection.upper() + '_PK PRIMARY KEY')
>>>>>>> feature-log-errors

                        if sql_table_exists(cursor, collection):
                            sql_drop_table(cursor, collection)

                        sql_create_table(cursor, collection, columns)

                        for index in indices:
                            cursor.execute("CREATE " + index)

<<<<<<< HEAD
                sql_add_foreign_keys(cursor, foreign_keys)
                self.commit()
=======
                        self.commit()

        except psycopg2.Error:
            LOG.error(u"A fatal error occured during tables creation")

            if not self.quiet:
                LOG.error(u"Traceback:\n%s", Traceback.format_exc())
>>>>>>> feature-log-errors

    def stop(self):
        pass

    def upsert(self, doc, namespace, timestamp):
        if not is_mapped(self.mappings, namespace):
            return

        try:
            with self.pgsql.cursor() as cursor:
                self._upsert(namespace, doc, cursor, timestamp)
                self.commit()

        except psycopg2.Error:
            LOG.error(u"Impossible to upsert %s to %s", doc, namespace)

            if not self.quiet:
                LOG.error(u"Traceback:\n%s", traceback.format_exc())

    def _upsert(self, namespace, document, cursor, timestamp):
        db, collection = db_and_collection(namespace)
        primary_key = self.mappings[db][collection]['pk']

        sql_delete_rows_where(cursor, collection, '{0} = {1}'.format(
            primary_key,
            to_sql_value(document[primary_key])
        ))

        sql_bulk_insert(cursor, self.mappings, namespace, [document])
        self.commit()

    def get_linked_tables(self, database, collection):
        linked_tables = []

        for field in self.mappings[database][collection]:
            field_mapping = self.mappings[database][collection][field]

            if 'fk' in field_mapping:
                linked_tables.append(field_mapping['dest'])

        return linked_tables

    def bulk_upsert(self, documents, namespace, timestamp):
        LOG.info('Inspecting %s...', namespace)

        if is_mapped(self.mappings, namespace):
            try:
                LOG.info('Mapping found for %s !...', namespace)
                LOG.info('Deleting all rows before update %s !...', namespace)

                db, collection = db_and_collection(namespace)
                for linked_table in self.get_linked_tables(db, collection):
                    sql_delete_rows(self.pgsql.cursor(), linked_table)

                sql_delete_rows(self.pgsql.cursor(), collection)
                self.commit()

                self._bulk_upsert(documents, namespace)
                LOG.info('%s done.', namespace)

            except psycopg2.Error:
                LOG.error(
                    "Impossible to bulk insert documents in namespace %s: %s",
                    namespace,
                    documents
                )

                if not self.quiet:
                    LOG.error("Traceback:\n%s", traceback.format_exc())

    def _bulk_upsert(self, documents, namespace):
        with self.pgsql.cursor() as cursor:
            document_buffer = []
            insert_accumulator = 0

            for document in documents:
                document_buffer.append(document)
                insert_accumulator += 1

                if insert_accumulator % self.chunk_size == 0:
                    sql_bulk_insert(cursor, self.mappings, namespace, document_buffer)

                    self.commit()
                    document_buffer = []

                    LOG.info('%s %s copied...', insert_accumulator, namespace)

            sql_bulk_insert(cursor, self.mappings, namespace, document_buffer)
            self.commit()

    def delete_from_array_field(self, document_id, db, collection):
        """
        Delete all rows related to a document from imbricated tables
        :param document_id:
        :param db:
        :param collection:
        :return:
        """

        for field in self.mappings[db][collection].keys():
            if field != 'pk':
                field_type = self.mappings[db][collection][field]['type']

                if field_type in [ARRAY_TYPE, ARRAY_OF_SCALARS_TYPE]:
                    dest = self.mappings[db][collection][field]['dest']
                    pk = self.mappings[db][dest]['pk']

                    sql_delete_rows_where(
                        self.pgsql.cursor(),
                        dest,
                        "{0} LIKE '{1}\_%'".format(
                            pk,
                            to_sql_value(document_id)
                        )
                    )

    def update(self, document_id, update_spec, namespace, timestamp):
        db, collection = db_and_collection(namespace)
        updated_document = self.get_document_by_id(db, collection, document_id)
        if updated_document is None:
            return

        try:
            self.delete_from_array_field(document_id, db, collection)

            with self.pgsql.cursor() as cursor:
                self._upsert(
                    namespace,
                    updated_document,
                    cursor,
                    timestamp
                )

                self.commit()

        except psycopg2.Error:
            LOG.error(
                u"Impossible to update document %s in namespace %s",
                document_id,
                namespace
            )

            if not self.quiet:
                LOG.error("Traceback:\n%s", traceback.format_exc())

    def get_document_by_id(self, db, collection, document_id):
        return self.client[db][collection].find_one({'_id': document_id})

    def remove(self, document_id, namespace, timestamp):
        if not is_mapped(self.mappings, namespace):
            return

        with self.pgsql.cursor() as cursor:
            db, collection = db_and_collection(namespace)
            primary_key = self.mappings[db][collection]['pk']
            cursor.execute(
                "DELETE from {0} WHERE {1} = '{2}';".format(collection.lower(), primary_key, str(document_id))
            )
            self.delete_from_array_field(document_id, db, collection)
            self.commit()
            LOG.info('Document with id {} from collection {} deleted', document_id, collection)

    def search(self, start_ts, end_ts):
        pass

    def commit(self):
        self.pgsql.commit()

    def get_last_doc(self):
        pass

    def handle_command(self, doc, namespace, timestamp):
        pass

    def prepare_mappings(self):
        # Set default values for dest fields
        for db in self.mappings:
            for collection in self.mappings[db]:
                for field in self.mappings[db][collection]:
                    # Exclude pk field (?? if this is the case, test field != 'pk' would be more explicit)
                    if isinstance(self.mappings[db][collection][field], dict):
                        if 'dest' not in self.mappings[db][collection][field]:
                            self.mappings[db][collection][field]['dest'] = field
