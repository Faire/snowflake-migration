import logging

from faire.internal.snowflake_client import client as snowflake_client

log = logging.getLogger()


class S3ToSnowflakeTransfer(object):

    DELIMS = {"TAB": "\t", "COMMA": ",", "PIPE": "|", "default": "\t"}

    def __init__(
        self,
        *,
        s3_bucket,
        s3_key,
        snowflake_dest_db,
        snowflake_dest_schema,
        snowflake_dest_table,
        snowflake_ddl,
        round_scales,
        field_optionally_enclosed_by,
        on_error,
        csv_file_delimiter,
        **_,
    ):
        self.s3_bucket = s3_bucket.lower()
        self.s3_key = s3_key.lower()
        self.s_dest_db = snowflake_dest_db
        self.s_dest_schema = snowflake_dest_schema
        self.s_dest_table = snowflake_dest_table
        self.snowflake_ddl = snowflake_ddl
        self.round_scales = round_scales
        self.field_optionally_enclosed_by = (
            "NONE"
            if field_optionally_enclosed_by.upper() == "NONE"
            else f"'{field_optionally_enclosed_by}'"
        )
        self.on_error = on_error
        self.csv_file_delimiter = self.DELIMS.get(
            csv_file_delimiter, self.DELIMS["default"]
        )

        self.s_landing_table = (
            f"{self.s_dest_db}.{self.s_dest_schema}.{self.s_dest_table}"
        )
        log.info("snowflake landing table: %s", self.s_landing_table)

    def create_s3_dest_table_in_snowflake(self):
        ddl = f"""
        CREATE OR REPLACE TABLE {self.s_landing_table} (
            {self.snowflake_ddl}
        );
        """
        snowflake_client.exec_sql(ddl)

    def load_from_s3_to_snowflake_table(self):
        copy_sql = f"""
        copy into {self.s_landing_table}
        from
        s3://{self.s3_bucket}/{self.s3_key}
        storage_integration = s3_int
        file_format = (
            TYPE = CSV FIELD_DELIMITER = '{self.csv_file_delimiter}'
            FIELD_OPTIONALLY_ENCLOSED_BY = {self.field_optionally_enclosed_by}
            )
        ON_ERROR = {self.on_error}
        ;
        """

        snowflake_client.exec_sql(copy_sql)

    def round_snowflake_table(self):
        round_sql = f"""
        CREATE OR REPLACE TABLE {self.s_landing_table} as
        select
            {self.round_scales}

        from {self.s_landing_table}
        ;
        """

        snowflake_client.exec_sql(round_sql)

    def transfer_s3_file_to_snowflake(self):
        log.info("Starting s3-to-snowflake transfer...")

        self.create_s3_dest_table_in_snowflake()

        self.load_from_s3_to_snowflake_table()

        if self.round_scales:
            self.round_snowflake_table()

        log.info("s3-to-snowflake transfer complete...")
        log.info("S3 file copied to: %s", self.s_landing_table)
