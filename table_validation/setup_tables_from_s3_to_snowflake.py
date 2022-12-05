import logging

# Has to be here to overwrite logging format from other modules
logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(module)s:%(funcName)s - %(message)s",
    level=logging.INFO,
)

import argparse
import pathlib

import yaml

try:
    from table_validation.s3_transfer import S3ToSnowflakeTransfer
except ModuleNotFoundError:
    from s3_transfer import S3ToSnowflakeTransfer


log = logging.getLogger()


def run(kwargs, dry_run=False):
    log.info(kwargs)

    # Setup validation table for Redshift S3 file
    redshift_s3_to_snowflake_transfer = S3ToSnowflakeTransfer(
        s3_bucket=kwargs["redshift_s3_bucket"],
        s3_key=kwargs["redshift_s3_key"],
        snowflake_dest_db=kwargs["snowflake_dest_db"],
        snowflake_dest_schema=kwargs["snowflake_dest_schema"],
        snowflake_dest_table=f"redshift_{kwargs['snowflake_dest_table']}",
        snowflake_ddl=kwargs["snowflake_ddl"],
        round_scales=kwargs["round_scales"],
        field_optionally_enclosed_by=kwargs["field_optionally_enclosed_by"],
        on_error=kwargs["on_error"],
        csv_file_delimiter=kwargs["csv_file_delimiter"],
    )
    if not dry_run:
        redshift_s3_to_snowflake_transfer.transfer_s3_file_to_snowflake()

    # Setup validation table for Snowflake S3 file
    snowflake_s3_to_snowflake_transfer = S3ToSnowflakeTransfer(
        s3_bucket=kwargs["snowflake_s3_bucket"],
        s3_key=kwargs["snowflake_s3_key"],
        snowflake_dest_db=kwargs["snowflake_dest_db"],
        snowflake_dest_schema=kwargs["snowflake_dest_schema"],
        snowflake_dest_table=f"snowflake_{kwargs['snowflake_dest_table']}",
        snowflake_ddl=kwargs["snowflake_ddl"],
        round_scales=kwargs["round_scales"],
        field_optionally_enclosed_by=kwargs["field_optionally_enclosed_by"],
        on_error=kwargs["on_error"],
        csv_file_delimiter=kwargs["csv_file_delimiter"],
    )
    if not dry_run:
        snowflake_s3_to_snowflake_transfer.transfer_s3_file_to_snowflake()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--redshift_s3_bucket", help="s3 bucket for redshift table")
    parser.add_argument("--redshift_s3_key", help="s3 key for redshift table")
    parser.add_argument("--snowflake_s3_bucket", help="s3 bucket for snowflake table")
    parser.add_argument("--snowflake_s3_key", help="s3 key for snowflake table")
    parser.add_argument("--snowflake_dest_table", help="dest table in snowflake")
    parser.add_argument(
        "--snowflake_ddl", help="snowflake ddl in snowflake for the s3 file"
    )
    parser.add_argument(
        "--snowflake_src_db", help="src db in snowflake", default="analytics"
    )
    parser.add_argument(
        "--snowflake_dest_db", help="dest db in snowflake", default="demo_db"
    )
    parser.add_argument(
        "--snowflake_dest_schema", help="dest db in snowflake", default="rafay"
    )

    parser.add_argument(
        "--round_scales",
        help="round scales to be applied for each column",
        default=None,
    )
    parser.add_argument(
        "--field_optionally_enclosed_by",
        help="value to use for field_optionally_enclosed_by for copy into",
        default='"',
    )
    parser.add_argument(
        "--on_error",
        help="value to use for on_error for copy into",
        default="ABORT_STATEMENT",  # this is also Snowflake's default option
    )
    parser.add_argument(
        "--csv_file_delimiter",
        help="delimiter to be used for parsing csv file. Valid values are TAB, COMMA and PIPE",
        default="TAB",
    )
    parser.add_argument(
        "-y", "--yaml", help="uses config.yaml instead of cli args", action="store_true"
    )

    args = parser.parse_args()

    # Args error generation
    if not args.yaml:
        reqd_args = (
            args.redshift_s3_bucket,
            args.redshift_s3_key,
            args.snowflake_s3_bucket,
            args.snowflake_s3_key,
            args.snowflake_dest_table,
        )
        if not all(reqd_args):
            raise Exception(
                "Args redshift_s3_bucket, redshift_s3_key, snowflake_s3_bucket, snowflake_s3_key and snowflake_dest_table need to be explicitly defined"
            )

    if args.yaml:
        # Extract config from yaml
        with open(
            f"{pathlib.Path(__file__).parent.absolute()}/config.yaml", "r"
        ) as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                log.error(exc)
    else:
        # Extract config from args
        config = vars(args)

    run(config, dry_run=False)
