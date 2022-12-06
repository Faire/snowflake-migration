import datetime
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
    from table_validation.redshift_transfer import RedshiftToSnowflakeTransfer
    from table_validation.snowflake_transfer import SnowflakeToSnowflakeTransfer
except ModuleNotFoundError:
    from redshift_transfer import RedshiftToSnowflakeTransfer
    from snowflake_transfer import SnowflakeToSnowflakeTransfer


log = logging.getLogger()


def run(kwargs, dry_run=False):
    log.info(kwargs)

    # Setup validation table for Redshift
    snowflake_transfer = RedshiftToSnowflakeTransfer(**kwargs)
    if not dry_run:
        snowflake_transfer.transfer_redshift_table_to_snowflake()

    # Setup validation table for Snowflake
    snowflake_internal_transfer = SnowflakeToSnowflakeTransfer(**kwargs)
    if not dry_run:
        snowflake_internal_transfer.do_transfer()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--redshift_src_schema", help="src schema in redshift")
    parser.add_argument("--redshift_src_table", help="src table in redshift")

    parser.add_argument("--snowflake_src_schema", help="src schema in snowflake")
    parser.add_argument("--snowflake_src_table", help="src table in snowflake")
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
        "--remove_quotes_from_create_table",
        help="Force remove quotes from create table statement when setting up a table in Snowflake for validation",
        default=False,
        type=bool,
    )

    parser.add_argument("--watermark_column", help="watermark column")
    parser.add_argument("--high_watermark", help="high watermark value")

    parser.add_argument(
        "-y", "--yaml", help="uses config.yaml instead of cli args", action="store_true"
    )

    args = parser.parse_args()

    # Args error generation
    if not args.yaml:
        watermark_values = (args.watermark_column, args.high_watermark)
        if any(watermark_values) and not all(watermark_values):
            raise Exception(
                "Both watermark_column and high_watermark need to be defined together"
            )

        reqd_args = (args.redshift_src_schema, args.redshift_src_table)
        if not all(reqd_args):
            raise Exception(
                "Args redshift_src_schema and redshift_src_table need to be explicitly defined"
            )

        # Set snowflake src schema / table to redshift if not provided
        args.snowflake_src_schema = (
            args.snowflake_src_schema or args.redshift_src_schema
        )
        args.snowflake_src_table = args.snowflake_src_table or args.redshift_src_table

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
        try:
            config["high_watermark"] = datetime.datetime.strptime(
                config["high_watermark"], "%Y-%m-%d"
            )
        except:
            pass

    run(config, dry_run=False)
