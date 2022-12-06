import json
import logging

import sqlparse
from faire.internal.snowflake_client import client as snowflake_client

log = logging.getLogger()


class ResultTable(object):
    def __init__(self, result_table):
        self.result_table = result_table

    def save(self, timestamp, url, result, schema, table):
        table_create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.result_table} (
        COMPLETED_AT TIMESTAMP_NTZ,
        SRC_SCHEMA VARCHAR,
        SRC_TABLE VARCHAR,
        URL VARCHAR,
        RESULT VARIANT
        );
        """

        # Replace single-quote with double quote to make it work for inserts
        escaped_result = json.dumps(result).replace("'", "").replace("\\n", "")

        sql = f"""
        {table_create_sql}

        INSERT INTO {self.result_table}
        SELECT COLUMN1 AS COMPLETED_AT, COLUMN2 AS SRC_SCHEMA,
        COLUMN3 AS SRC_TABLE, COLUMN4 AS URL, PARSE_JSON(COLUMN5) AS RESULT
        FROM VALUES ('{timestamp}', '{schema}', '{table}', '{url}', '{escaped_result}')
                 AS VALS;
        """

        log.info(sqlparse.format(sql))

        snowflake_client.exec_sql_multi(sql, parse_from_redshift=False)

        log.info(f"Result stored in {result}")
