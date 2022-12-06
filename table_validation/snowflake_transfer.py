import logging

import sqlparse
from faire.internal.snowflake_client import client as snowflake_client

log = logging.getLogger()


class SnowflakeToSnowflakeTransfer(object):
    def __init__(
        self,
        *,
        snowflake_src_db,
        snowflake_src_schema,
        snowflake_src_table,
        snowflake_dest_db,
        snowflake_dest_schema,
        watermark_column=None,
        high_watermark=None,
        **_,
    ):
        self.src_db = snowflake_src_db
        self.src_schema = snowflake_src_schema
        self.src_table = snowflake_src_table
        self.dest_db = snowflake_dest_db
        self.dest_schema = snowflake_dest_schema
        self.watermark_column = watermark_column
        self.watermark = high_watermark.strftime("%Y-%m-%d") if high_watermark else None

        self.landing_table = f"{self.dest_db}.{self.dest_schema}.snowflake_{self.src_schema}_{self.src_table}"
        log.info("Snowflake landing table: %s", self.landing_table)

    def do_transfer(self):
        log.info("Starting snowflake internal transfer...")

        column = self.watermark_column or 1  # Neat query construct trick
        watermark = (
            f"'{self.watermark}'" if self.watermark else 1
        )  # Neat query construct trick

        copy_sql = f"""
        CREATE OR REPLACE TABLE {self.landing_table} AS
        WITH VALIDATION_TABLE AS (
            SELECT *
            FROM {self.src_db}.{self.src_schema}.{self.src_table}
            WHERE {column} <= {watermark}
        )
        SELECT *
        FROM VALIDATION_TABLE;
        """
        log.info(sqlparse.format(copy_sql))

        snowflake_client.exec_sql(copy_sql, parse_from_redshift=False)

        log.info("snowflake internal transfer complete...")
        log.info("Snowflake data copied to: %s", self.landing_table)
