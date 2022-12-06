import logging
import re

import sqlparse
from faire.internal.snowflake_client import client as snowflake_client
from faire.internal.vendor.aws import redshift

log = logging.getLogger()

special_chars = re.compile(r"[^a-zA-Z_\s\d,\"\(\)]")

S3_BUCKET = "YOUR_BUCKET"
IAM_ROLE = "IAM_ROLE"
SNOWFLAKE_STAGE = "SNOWFLAKE_STAGE"


def contains_special_chars(line):
    return bool(special_chars.search(line))


def standardize_columns(line, remove_quotes_from_create_table):

    if remove_quotes_from_create_table and "CREATE TABLE IF NOT EXISTS" in line:
        return line.replace('"', "").replace("'", "")

    # if a line contains these special characters, columns should be kept enclosed with quotes
    if contains_special_chars(line):
        return line
    else:
        return line.replace('"', "").replace(
            "'", ""
        )  # quotes around column names force lowercase in snowflake


class RedshiftToSnowflakeTransfer(object):
    def __init__(
        self,
        *,
        redshift_src_schema,
        redshift_src_table,
        snowflake_dest_db,
        snowflake_dest_schema,
        remove_quotes_from_create_table,
        watermark_column=None,
        high_watermark=None,
        **_,
    ):
        self.r_src_schema = redshift_src_schema.lower()
        self.r_src_table = redshift_src_table.lower()
        self.s_dest_db = snowflake_dest_db
        self.s_dest_schema = snowflake_dest_schema
        self.remove_quotes_from_create_table = remove_quotes_from_create_table
        self.watermark_column = watermark_column
        self.watermark = high_watermark.strftime("%Y-%m-%d") if high_watermark else None

        self.s_landing_table = f"{self.s_dest_db}.{self.s_dest_schema}.redshift_{self.r_src_schema}_{self.r_src_table}"
        log.info("Redshift landing table: %s", self.s_landing_table)

    def get_ddl_for_snowlake(self):
        res = redshift.exec_sql(
            f"""select * from table_definition where schemaname = '{self.r_src_schema}'
            and tablename = '{self.r_src_table}';"""
        )

        result = res.fetchall()
        ddl = [row[2] for row in result]
        res.close()

        log.info("Starting ddl parsing...")

        # Parsed ddl is coupled with alter and drop statements which aren't needed
        parsed_ddl = [
            standardize_columns(line, self.remove_quotes_from_create_table)
            for line in ddl
            if not (line.startswith("ALTER TABLE") or line.startswith("--DROP TABLE"))
        ]

        ddl = "\n".join(parsed_ddl)
        log.info("Redshift ddl")
        log.info(sqlparse.format(ddl, reindent=False))

        snowflake_ddl = ddl.replace(
            f"CREATE TABLE IF NOT EXISTS {self.r_src_schema}.{self.r_src_table}",
            f"CREATE OR REPLACE TABLE {self.s_landing_table}",
        )
        log.info("-- Snowflake ddl")
        log.info(sqlparse.format(snowflake_ddl, reindent=False))

        return snowflake_ddl

    def unload_redshift_table_to_s3(self):
        schema = self.r_src_schema
        table = self.r_src_table
        column = self.watermark_column or "1"  # Neat query construct trick
        watermark = (
            f"'{self.watermark}'" if self.watermark else "1"
        )  # Neat query construct trick
        bucket = watermark.replace("'", "")

        unload_sql = f"""
        unload ($$select * from {schema}.{table}
        where {column} <= {watermark}$$)
        to 's3://{S3_BUCKET}/unload/snowflake_parity/nsp={schema}/{table}/{bucket}/'
        iam_role '{IAM_ROLE}'
        delimiter '|'
        addquotes
        null 'NULL'
        escape
        maxfilesize 100MB
        gzip
        cleanpath;
        """

        redshift.exec_sql(unload_sql)

    def create_redshift_dest_table_in_snowflake(self):
        snowflake_client.exec_sql(self.get_ddl_for_snowlake())

    def load_from_s3_to_snowflake_table(self):
        bucket = (
            self.watermark or "1"
        )  # This kind of bucket means it was loaded without watermark

        copy_sql = fr"""
        copy into {self.s_landing_table}
        from
        @{SNOWFLAKE_STAGE}/unload/snowflake_parity/nsp={self.r_src_schema}/{self.r_src_table}/{bucket}/
        file_format = (
        type = csv
        field_delimiter = '|'
        field_optionally_enclosed_by = '"'
        escape = '\\'
        null_if = ('NULL')
        empty_field_as_null = False
        );
        """

        snowflake_client.exec_sql(copy_sql)

    def transfer_redshift_table_to_snowflake(self):
        log.info("Starting redshift-to-snowflake transfer...")

        self.unload_redshift_table_to_s3()

        self.create_redshift_dest_table_in_snowflake()

        self.load_from_s3_to_snowflake_table()

        log.info("redshift-to-snowflake transfer complete...")
        log.info("Redshift data copied to: %s", self.s_landing_table)
