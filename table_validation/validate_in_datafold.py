import logging

# Has to be here to overwrite logging format from other modules
logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(module)s:%(funcName)s - %(message)s",
    level=logging.INFO,
)

import argparse
import datetime
import json
import os
import re

try:
    from table_validation.datafold_api import DatafoldApi, DatafoldApiError
    from table_validation.result_table import ResultTable
except ModuleNotFoundError:
    from datafold_api import DatafoldApi
    from result_table import ResultTable

log = logging.getLogger()


def call_datafold(kwargs):
    api = DatafoldApi(**kwargs)

    url, result = api.run_diff(
        table1=kwargs.get("table1"),
        table2=kwargs.get("table2"),
        query1=kwargs.get("query1"),
        query2=kwargs.get("query2"),
        pks=kwargs["pks"],
        exclude_columns=kwargs.get("exclude_columns"),
        filter=kwargs.get("filter"),
        sampling_tolerance=kwargs.get("sampling_tolerance"),
        sampling_confidence=kwargs.get("sampling_confidence"),
        diff_tolerances_per_column=kwargs.get("diff_tolerances_per_column"),
    )

    # extract the landing table from a string like demo_db.test.some_redshift_table
    _, _, dest_table = kwargs["table1"].split(".")

    # extract from a string like some_redshift_table
    supported_db = ["redshift", "snowflake"]

    # List of schemas to support validation
    supported_schema = [
        "s3_file",
        "production",
    ]
    try:
        matched_groups = re.match(
            fr"({'|'.join(supported_db)})_({'|'.join(supported_schema)})_([\w+_]+)",
            dest_table,
        )
        db, schema, table = matched_groups.group(1, 2, 3)
    except AttributeError:
        _msg = f"Cannot recognize db or schema in {dest_table}"
        log.error(_msg)
        raise Exception(_msg)

    result_table = ResultTable(result_table=kwargs.get("result_table"))
    result_table.save(
        timestamp=datetime.datetime.utcnow().isoformat(),
        url=url,
        result=result,
        schema=schema,
        table=table,
    )

    if result.get("error"):
        raise DatafoldApiError(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Call the DataFold API to validate a pair of tables, one in Redshift and one in Snowflake.",
        usage="""\n$ python validate_in_datafold.py demo_db.test.some_redshift_table demo_db.test.some_snowflake_table pk1 pk2 pk3 --exclude_columns ETL_CREATED_AT FOOBAR document_created_at --diff_tolerance_per_column '{"column_name": "FOO", "tolerance_value": "0.01", "tolerance_mode": "absolute"}' '{"column_name": "FOO2", "tolerance_value": "0.01", "tolerance_mode": "absolute"}'""",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "table1",
        help="fully resolved table 1 in Snowflake."
        "eg. demo_db.rafay.redshift_etl_core_products",
    )
    parser.add_argument(
        "table2",
        help="fully resolved table 2 in Snowflake. "
        "eg. demo_db.rafay.snowflake_etl_core_products",
    )
    parser.add_argument(
        "pks", nargs="+", help="list of primary keys to be used in datafold"
    )
    parser.add_argument(
        "--query1",
        help="Fully qualified query1 to be used instead of table1. Note that specifying query would take precedence "
        "over a table",
    )
    parser.add_argument(
        "--query2",
        help="Fully qualified query2 to be used instead of table2. Note that specifying query would take precedence "
        "over a table",
    )
    parser.add_argument(
        "--exclude_columns",
        nargs="+",
        help="columns that should be excluded from the validation result",
    )
    parser.add_argument(
        "--filter", help="it'll be injected as-is to WHERE clauses during diffing."
    )

    parser.add_argument(
        "--sampling_tolerance",
        help="ratio of acceptable number of rows that can be different to the total number of rows.",
        type=float,
    )

    parser.add_argument(
        "--sampling_confidence",
        help="confidence that number of differing rows is less than allowed by sampling_tolerance.",
        type=float,
    )

    parser.add_argument(
        "--diff_tolerances_per_column",
        nargs="+",
        help="List of json dicts to configure tolerance value per column: ",
        type=json.loads,
    )

    parser.add_argument(
        "--result_table",
        help="table where result from datafold should be stored",
        default="demo_db.diff.diff_runs",
    )

    args = parser.parse_args()

    config = vars(args)
    log.info("Config received: %s", config)

    config["api_key"] = os.environ.get("DATAFOLD_API_KEY")
    config["datafold_datasource_id"] = os.environ.get("DATAFOLD_DATASOURCE_ID")

    if not all((config["api_key"], config["datafold_datasource_id"])):
        msg = "Datafold parameters are not defined in environment variables!"
        log.error(msg)
        raise RuntimeError(msg)

    call_datafold(config)
