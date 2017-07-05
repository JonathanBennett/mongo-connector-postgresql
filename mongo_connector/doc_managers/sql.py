# coding: utf8

import logging
import unicodedata

import re
import traceback
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
    get_nested_field_from_document,
    flatten_query_tree,
    Atom,
    ARRAY_OF_SCALARS_TYPE,
    ARRAY_TYPE
)


LOG = logging.getLogger(__name__)


all_chars = (chr(i) for i in range(0x10000))
control_chars = ''.join(c for c in all_chars if unicodedata.category(c) == 'Cc')
control_char_re = re.compile('[%s]' % re.escape(control_chars))


class ForeignKey(Atom):
    pass


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
    sql = u"DROP TABLE IF EXISTS {0} CASCADE".format(tableName.lower())
    cursor.execute(sql)


def sql_create_table(cursor, tableName, columns):
    columns.sort()
    sql = u"CREATE TABLE {0} {1}".format(tableName.lower(), to_sql_list(columns))
    cursor.execute(sql)


def sql_add_foreign_keys(cursor, foreign_keys):
    fmt = 'ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({}) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED'

    for foreign_key in foreign_keys:
        cmd = fmt.format(
            foreign_key['table'],
            '{0}_{1}_fk'.format(foreign_key['table'], foreign_key['fk']),
            foreign_key['fk'],
            foreign_key['ref'],
            foreign_key['pk']
        )
        cursor.execute(cmd)


def sql_bulk_insert(cursor, mappings, namespace, documents):
    query = []
    _sql_bulk_insert(query, mappings, namespace, documents)
    query = flatten_query_tree(query)

    with_stmts = []
    final_stmt = ''

    for subquery in query:
        keyvals = dict(zip(subquery['keys'], subquery['values']))
        foreign_keys = {}
        values = {}

        for key in keyvals:
            val = keyvals[key]

            if isinstance(val, ForeignKey):
                foreign_keys[key] = val.split('.')[1]

            else:
                values[key] = val

        data_alias = '{0}_data_{1}'.format(
            subquery['collection'],
            subquery['idx']
        )
        rows_alias = '{0}_rows_{1}'.format(
            subquery['collection'],
            subquery['idx']
        )
        subquery['alias'] = {
            'data': data_alias,
            'rows': rows_alias
        }

        with_stmts.append(
            '{alias} ({columns}) AS (VALUES ({values}))'.format(
                alias=data_alias,
                columns=','.join(values.keys()),
                values=','.join(values.values())
            )
        )

        keys = ','.join(list(values.keys()) + list(foreign_keys.keys()))
        projection = [
            '{0}.{1} AS {1}'.format(data_alias, key)
            for key in values.keys()
        ]
        aliases = [data_alias]

        if 'parent' in subquery:
            psubquery = query[subquery['parent']]
            parent_rows_alias = psubquery['alias']['rows']

            projection += [
                '{0}.{1} AS {2}'.format(
                    parent_rows_alias,
                    foreign_keys[key],
                    key
                )
                for key in foreign_keys
            ]
            aliases.append(parent_rows_alias)

        projection = ','.join(projection)
        aliases = ','.join(aliases)

        if not subquery['last']:
            with_stmts.append(
                '{alias} AS (INSERT INTO {table} ({columns}) SELECT {projection} FROM {aliases} RETURNING {pk})'.format(
                    alias=rows_alias,
                    table=subquery['collection'],
                    columns=keys,
                    projection=projection,
                    aliases=aliases,
                    pk=subquery['pk']
                )
            )

        else:
            final_stmt = 'INSERT INTO {table} ({columns}) SELECT {projection} FROM {aliases}'.format(
                table=subquery['collection'],
                columns=keys,
                projection=projection,
                aliases=aliases
            )

    sql = 'WITH {0} {1}'.format(
        ','.join(with_stmts),
        final_stmt
    )
    cursor.execute(sql)


def _sql_bulk_insert(query, mappings, namespace, documents):
    if not documents:
        return

    db, collection = db_and_collection(namespace)

    primary_key = mappings[db][collection]['pk']
    keys = [
        v['dest'] for k, v in iteritems(mappings[db][collection])
        if 'dest' in v
        and v['type'] not in [ARRAY_TYPE, ARRAY_OF_SCALARS_TYPE]
    ]
    keys.sort()

    for document in documents:
        mapped_document = get_mapped_document(mappings, document, namespace)
        values = [
            to_sql_value(extract_creation_date(mapped_document, primary_key))
        ]

        for key in keys:
            if key in mapped_document:
                values.append(to_sql_value(mapped_document[key]))

            else:
                values.append(to_sql_value(None))

        subquery = {
            'collection': collection,
            'keys': ['_creationDate'] + keys,
            'values': values,
            'pk': primary_key,
            'queries': []
        }
        query.append(subquery)

        insert_document_arrays(
            collection,
            subquery['queries'],
            db,
            document,
            mapped_document,
            mappings,
            primary_key
        )
        insert_scalar_arrays(
            collection,
            subquery['queries'],
            db,
            document,
            mapped_document,
            mappings,
            primary_key
        )

def insert_scalar_arrays(collection, query, db, document, mapped_document, mappings, primary_key):
    pk = mapped_document.get(
        primary_key,
        ForeignKey('{0}.{1}'.format(collection, primary_key))
    )

    for arrayField in get_array_of_scalar_fields(mappings, db, collection, document):
        dest = mappings[db][collection][arrayField]['dest']
        fk = mappings[db][collection][arrayField]['fk']
        pk = mappings[db][dest]['pk']
        value_field = mappings[db][collection][arrayField]['valueField']
        scalar_values = get_nested_field_from_document(document, arrayField)

        linked_documents = []

        for value in scalar_values:
            linked_documents.append({fk: pk, value_field: value})

        _sql_bulk_insert(query, mappings, "{0}.{1}".format(db, dest), linked_documents)


def insert_document_arrays(collection, query, db, document, mapped_document, mappings, primary_key):
    pk = mapped_document.get(
        primary_key,
        ForeignKey('{0}.{1}'.format(collection, primary_key))
    )

    for arrayField in get_array_fields(mappings, db, collection, document):
        dest = mappings[db][collection][arrayField]['dest']
        fk = mappings[db][collection][arrayField]['fk']
        pk = mappings[db][dest]['pk']
        linked_documents = get_nested_field_from_document(document, arrayField)

        count = 0
        for linked_document in linked_documents:
            linked_document[fk] = pk

        _sql_bulk_insert(query, mappings, "{0}.{1}".format(db, dest), linked_documents)


def get_document_keys(document):
    keys = list(document)
    keys.sort()

    return keys


def remove_control_chars(s):
    return control_char_re.sub('', s)


def to_sql_value(value):
    if value is None:
        return 'NULL'

    if isinstance(value, (int, long, float, complex)):
        return str(value)

    if isinstance(value, bool):
        return str(value).upper()

    if isinstance(value, Atom):
        return value

    if isinstance(value, basestring):
        return u"'{0}'".format(remove_control_chars(value).replace("'", "''"))

    return u"'{0}'".format(str(value))


def object_id_adapter(object_id):
    return AsIs(to_sql_value(object_id))
