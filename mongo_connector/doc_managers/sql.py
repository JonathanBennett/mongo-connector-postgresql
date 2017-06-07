# coding: utf8

import logging
import unicodedata

import re
from builtins import chr
from future.utils import iteritems
from past.builtins import long, basestring
from psycopg2._psycopg import AsIs

from mongo_connector.doc_managers.mappings import (
    get_mapped_document,
    get_transformed_value,
    get_transformed_document
)

from mongo_connector.doc_managers.utils import (
    extract_creation_date,
    get_array_fields,
    db_and_collection,
    get_array_of_scalar_fields,
    ARRAY_OF_SCALARS_TYPE,
    ARRAY_TYPE,
    get_nested_field_from_document
)


LOG = logging.getLogger(__name__)


all_chars = (chr(i) for i in range(0x10000))
control_chars = ''.join(c for c in all_chars if unicodedata.category(c) == 'Cc')
control_char_re = re.compile('[%s]' % re.escape(control_chars))


def to_sql_list(items):
    return ' ({0}) '.format(','.join(items))


def sql_table_exists(cursor, table):
    cursor.execute(""
                   "SELECT EXISTS ("
                   "        SELECT 1 "
                   "FROM   information_schema.tables "
                   "WHERE  table_schema = 'public' "
                   "AND    table_name = '" + table.lower() + "' );")
    return cursor.fetchone()[0]


def sql_delete_rows(cursor, table):
    cursor.execute(u"DELETE FROM {0}".format(table.lower()))


def sql_delete_rows_where(cursor, table, where_clause):
    cursor.execute(u"DELETE FROM {0} WHERE {1}".format(table.lower(), where_clause))


def sql_drop_table(cursor, tableName):
    sql = u"DROP TABLE {0}".format(tableName.lower())
    cursor.execute(sql)


def sql_create_table(cursor, tableName, columns):
    columns.sort()
    sql = u"CREATE TABLE {0} {1}".format(tableName.lower(), to_sql_list(columns))
    cursor.execute(sql)


def sql_add_foreign_keys(cursor, foreign_keys):
    fmt = 'ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({})'

    for foreign_key in foreign_keys:
        print(foreign_key)
        cmd = fmt.format(
            foreign_key['table'],
            '{0}_{1}_fk'.format(foreign_key['table'], foreign_key['fk']),
            foreign_key['fk'],
            foreign_key['ref'],
            foreign_key['pk']
        )
        print(cmd)
        cursor.execute(cmd)


def sql_bulk_insert(cursor, mappings, namespace, documents):
    if not documents:
        return

    db, collection = db_and_collection(namespace)

    primary_key = mappings[db][collection]['pk']
    mapped_fields = {
        mapping['dest']: mapping
        for _, mapping in iteritems(mappings[db][collection])
        if 'dest' in mapping and mapping['type'] not in (
            ARRAY_TYPE,
            ARRAY_OF_SCALARS_TYPE
        )
    }
    keys = list(mapped_fields.keys())

    # Adding primary_key for imbricated document where they have no 'dest' field
    if primary_key not in keys:
        keys.append(primary_key)

    keys.sort()
    values = []

    for document in documents:
        mapped_document = get_mapped_document(mappings, document, namespace)

        # Add primary key value generated for imbricated tables
        if primary_key not in mapped_document:
            mapped_document[primary_key] = document[primary_key]

        document_values = [
            to_sql_value(
                extract_creation_date(
                    mapped_document,
                    primary_key
                )
            )
        ]

        if not mapped_document:
            break

        for key in keys:
            if key in mapped_document:
                val = get_transformed_value(
                    mapped_fields.get(key, {}),
                    mapped_document, key
                )
                document_values.append(to_sql_value(val))

            else:
                document_values.append(to_sql_value(None))
        values.append(u"({0})".format(u','.join(document_values)))

        insert_document_arrays(collection, cursor, db, document, mapped_document, mappings, primary_key)
        insert_scalar_arrays(collection, cursor, db, document, mapped_document, mappings, primary_key)

    if values:
        sql = u"INSERT INTO {0} ({1}) VALUES {2}".format(
            collection,
            u','.join(['_creationDate'] + keys),
            u",".join(values)
        )
        cursor.execute(sql)


def insert_scalar_arrays(collection, cursor, db, document, mapped_document, mappings, primary_key):
    for arrayField in get_array_of_scalar_fields(mappings, db, collection, document):
        dest = mappings[db][collection][arrayField]['dest']
        fk = mappings[db][collection][arrayField]['fk']
        pk = mappings[db][dest]['pk']
        value_field = mappings[db][collection][arrayField]['valueField']
        scalar_values = get_nested_field_from_document(document, arrayField)

        linked_documents = []

        count = 0
        for value in scalar_values:
            # Generate automatically primary_key for linked documents
            linked_pk = str(mapped_document[primary_key]) + '_' + str(count)
            document = {pk: linked_pk,
                        fk: mapped_document[primary_key],
                        value_field: value}
            count += 1
            linked_documents.append(document)
        sql_bulk_insert(cursor, mappings, "{0}.{1}".format(db, dest), linked_documents)


def insert_document_arrays(collection, cursor, db, document, mapped_document, mappings, primary_key):
    for arrayField in get_array_fields(mappings, db, collection, document):
        dest = mappings[db][collection][arrayField]['dest']
        fk = mappings[db][collection][arrayField]['fk']
        pk = mappings[db][dest]['pk']
        linked_documents = get_nested_field_from_document(document, arrayField)

        count = 0
        for linked_document in linked_documents:
            linked_document[fk] = mapped_document[primary_key]
            # Generate automatically primary_key for linked documents
            linked_document[pk] = str(mapped_document[primary_key]) + '_' + str(count)
            count += 1

        sql_bulk_insert(cursor, mappings, "{0}.{1}".format(db, dest), linked_documents)


def get_document_keys(document):
    keys = list(document)
    keys.sort()

    return keys


def sql_insert(cursor, tableName, document, mappings, db, collection):
    primary_key = mappings[db][collection]['pk']

    creationDate = extract_creation_date(document, primary_key)
    if creationDate is not None:
        document['_creationDate'] = creationDate

    keys = get_document_keys(document)
    valuesPlaceholder = ("%(" + column_name + ")s" for column_name in keys)

    if primary_key in document:
        sql = u"INSERT INTO {0} {1} VALUES {2} ON CONFLICT ({3}) DO UPDATE SET {1} = {2}".format(
            tableName,
            to_sql_list(keys),
            to_sql_list(valuesPlaceholder),
            primary_key
        )
    else:
        sql = u"INSERT INTO {0} {1} VALUES {2}".format(
            tableName,
            to_sql_list(keys),
            to_sql_list(valuesPlaceholder),
            primary_key
        )

    try:
        cursor.execute(
            sql,
            get_transformed_document(mappings, db, collection, document)
        )
    except Exception as e:
        LOG.error(u"Impossible to upsert the following document %s : %s", document, e)


def remove_control_chars(s):
    return control_char_re.sub('', s)


def to_sql_value(value):
    if value is None:
        return 'NULL'

    if isinstance(value, (int, long, float, complex)):
        return str(value)

    if isinstance(value, bool):
        return str(value).upper()

    if isinstance(value, basestring):
        return u"'{0}'".format(remove_control_chars(value).replace("'", "''"))

    return u"'{0}'".format(str(value))


def object_id_adapter(object_id):
    return AsIs(to_sql_value(object_id))
