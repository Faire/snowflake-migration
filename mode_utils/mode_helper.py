#!/usr/bin/env python3
import sys

import argparse
import json
import logging
import os
import re
import requests
from requests.auth import HTTPBasicAuth
from parser.snowflake_parser import parse_redshift_to_snowflake
import time

MODE_HOST = "https://app.mode.com"

REDSHIFT_DS_ID = 12345
SNOWFLAKE_DS_ID = 12345

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

s = requests.Session()
s.auth = HTTPBasicAuth(os.environ["MODE_API_TOKEN"], os.environ["MODE_API_PASSWORD"])
s.headers.update({"Content-Type": "application/json", "Accept": "application/hal+json"})


def mode_api(method, path, request_args=None):
    if request_args is None:
        request_args = {}

    # fix up some paths returned from mode API
    if path.startswith("/<your-organization>"):
        path = f"/api{path}"

    # allow omitting prefix for ease of use
    if not path.startswith("/api/<your-organization>"):
        path = f"/api/<your-organization>{path}"

    request_args["url"] = f"{MODE_HOST}{path}"
    logger.info(f"Sending {method} to {path}")
    response = getattr(s, method)(**request_args)
    # abort if there were any errors
    response.raise_for_status()
    # unwrap response if applicable
    return response.json().get("_embedded", response.json())


# recursively walk the provided view, trying to match the provided path
# when we get to the end of the provided path, capitalize that element/list/dict
def capitalize_view_field(view, *path):
    if len(path) > 1:
        if path[0] == "*":
            # capitalize all list members
            for item in view:
                capitalize_view_field(item, *path[1:])
        elif path[0] in view:
            capitalize_view_field(view[path[0]], *path[1:])
    elif path[0] in view:
        if isinstance(view[path[0]], str):
            view[path[0]] = view[path[0]].upper()
        elif isinstance(view[path[0]], list):
            view[path[0]] = [x.upper() for x in view[path[0]]]
        elif isinstance(view[path[0]], dict):
            view[path[0]] = {k.upper(): v for k, v in view[path[0]].items()}


# try to capitalize a series of paths inside view/view_vegas if they exist
def capitalize_view_fields(view):
    capitalize_view_field(view, "area", "label")
    capitalize_view_field(view, "area", "x")
    capitalize_view_field(view, "area", "y")
    capitalize_view_field(view, "bar", "x")
    capitalize_view_field(view, "bar", "y")
    capitalize_view_field(view, "bigNumber", "column")
    capitalize_view_field(view, "bigNumber", "columns")
    capitalize_view_field(view, "encoding", "column", "*", "formula")
    capitalize_view_field(view, "encoding", "filter", "*", "formula")
    capitalize_view_field(
        view,
        "encoding",
        "marks",
        "series",
        "*",
        "properties",
        "color",
        "values",
        "*",
        "formula",
    )

    capitalize_view_field(
        view, "encoding", "marks", "properties", "angle", "values", "*", "formula"
    )
    capitalize_view_field(
        view, "encoding", "marks", "properties", "color", "values", "*", "formula"
    )
    capitalize_view_field(
        view, "encoding", "marks", "properties", "detail", "values", "*", "formula"
    )
    capitalize_view_field(
        view, "encoding", "marks", "properties", "label", "values", "*", "formula"
    )
    capitalize_view_field(
        view, "encoding", "marks", "properties", "size", "values", "*", "formula"
    )
    capitalize_view_field(
        view, "encoding", "marks", "properties", "text", "values", "*", "formula"
    )
    capitalize_view_field(
        view, "encoding", "marks", "properties", "tooltip", "values", "*", "formula"
    )

    capitalize_view_field(view, "encoding", "sorts", "*", "source")
    capitalize_view_field(view, "encoding", "value", "*", "formula")
    capitalize_view_field(view, "encoding", "x", "*", "formula")
    capitalize_view_field(view, "encoding", "x2", "*", "formula")
    capitalize_view_field(view, "encoding", "y", "*", "formula")
    capitalize_view_field(view, "encoding", "y2", "*", "formula")
    capitalize_view_field(view, "fieldFormats")
    capitalize_view_field(view, "format", "flatTable", "columns", "*", "id")
    capitalize_view_field(view, "line", "label")
    capitalize_view_field(view, "line", "x")
    capitalize_view_field(view, "line", "y")
    capitalize_view_field(view, "linePlusBar", "label")
    capitalize_view_field(view, "linePlusBar", "x")
    capitalize_view_field(view, "linePlusBar", "y")
    capitalize_view_field(view, "linePlusBar", "ybars")
    capitalize_view_field(view, "linePlusBar", "ylines")
    capitalize_view_field(view, "pie", "label")
    capitalize_view_field(view, "pie", "value")
    capitalize_view_field(view, "pivotTable", "filterValues")
    capitalize_view_field(view, "scatter", "label")
    capitalize_view_field(view, "scatter", "x")
    capitalize_view_field(view, "scatter", "y")
    return view


def validate_report(report):
    logger.info(f'==== validating report {report["token"]}')
    report_queries = mode_api("get", f'/reports/{report["token"]}/queries')

    banned_queries = ["create table", "insert into", "drop table"]

    has_redshift_queries = False
    for query in report_queries["queries"]:
        if query["data_source_id"] == REDSHIFT_DS_ID:
            has_redshift_queries = True
            for banned_query in banned_queries:
                if re.search(
                    banned_query.replace(" ", r"\s+"),
                    query["raw_query"].replace(r"\n", " "),
                ):
                    sys.exit(
                        f"This report cannot be migrated because it uses a banned query '{banned_query}', please contact #snowflake-migration-mode for next steps."
                    )

    if not has_redshift_queries:
        sys.exit("This report does not use Redshift, so no migration is needed.")

    report_collection = mode_api("get", f'/spaces/{report["space_token"]}')
    if report_collection["space_type"] == "private":
        sys.exit(
            "Reports cannot be cloned from a private collection. Please move the report to a public collection, then try again."
        )


def clone_report(report_token, prefix):
    original_report = mode_api("get", f"/reports/{report_token}")
    validate_report(original_report)

    cloned_report = None
    logger.info(f"==== cloning report {report_token}")
    try:
        cloned_report = mode_api(
            original_report["_forms"]["clone"]["method"],
            original_report["_forms"]["clone"]["action"],
        )

    except requests.exceptions.HTTPError as err:
        # if cloning the report takes over 60 seconds then we get back a 503 from Mode but the report actually does get
        # cloned into the caller's personal space, so try to look it up by name there instead
        if err.response.status_code == 503:
            logger.info(
                "Received a 503 when cloning report, attempting to look up by name instead..."
            )
            # time.sleep(5)
            spaces = mode_api("get", "/spaces")
            for space in spaces["spaces"]:
                if space["name"] == "Personal":
                    # when the report is still cloning the name is null, so wait for it to be populated
                    while True:
                        time.sleep(10)
                        personal_reports = mode_api(
                            "get",
                            f'{space["_links"]["reports"]["href"]}?order=desc&order_by=created_at',
                        )
                        # the cloned report should be the first one in the list
                        latest_report = personal_reports["reports"][0]
                        if latest_report["name"] is None:
                            # report is still cloning, try again
                            continue
                        elif (
                            latest_report["name"]
                            == f'Clone of {original_report["name"]}'
                        ):
                            cloned_report = latest_report
                        # we found the report, or something went wrong
                        break

        if cloned_report is None:
            logger.info(
                "Unable to look up cloned report by name, error when cloning was:"
            )
            raise

    cloned_report_token = cloned_report["token"]

    # rename the cloned report, and move to original report's collection
    # truncate name at 64 character limit imposed by Mode to avoid 400
    cloned_report["name"] = f"{prefix} {original_report['name']}"[:64]

    cloned_report["space_token"] = original_report["space_token"]
    # if you don't want to move the report to the original space, remove the space_token before patching to avoid a 404
    # cloned_report.pop("space_token", None)

    report_update_body = {"report": cloned_report}
    mode_api("patch", f"/reports/{cloned_report_token}", {"json": report_update_body})

    # ensure the report can be run manually
    update_settings_body = {
        "report": {
            "theme": cloned_report["_forms"]["update_settings"]["input"]["report"][
                "theme"
            ]["value"],
            "full_width": cloned_report["_forms"]["update_settings"]["input"]["report"][
                "full_width"
            ]["value"],
            "manual_run_disabled": False,
        }
    }
    mode_api(
        cloned_report["_forms"]["update_settings"]["method"],
        cloned_report["_forms"]["update_settings"]["action"],
        {"json": update_settings_body},
    )

    return cloned_report_token


def update_query_sql_syntax(raw_query):
    # Mode API returns a 400 when you try to update a completely empty query, so add a comment instead (which will also
    # fail in snowflake, but at least the script will finish...)
    if raw_query == "":
        raw_query = "-- empty query"

    # find quoted aliased columns and uppercase all instances of them to match chart updates
    aliased_columns = re.findall(
        r"""(as\s+)("[^"]+"|'[^']+')([^\s]*?\s)""", raw_query, flags=re.IGNORECASE
    )
    for aliased_column in aliased_columns:
        # match contains 3 groups, we want the middle one
        column_id = aliased_column[1]
        raw_query = raw_query.replace(column_id, column_id.upper())

    # query groups are not supported in snowflake, so comment them out
    raw_query = re.sub(
        r"^set query_group to", "-- set query_group to", raw_query, flags=re.MULTILINE
    )
    raw_query = re.sub(
        r"^reset query_group", "-- reset query_group", raw_query, flags=re.MULTILINE
    )

    # migrate snowflake mode_etls
    mode_etls = [
        "finance_customer_ops_and_fulfillment_costs",
        "finance_daily_gmv_lfc",
        "finance_daily_gmv_h2plan",
        "finance_daily_gmv_projection",
        "finance_interest_expense",
        "finance_reserves",
    ]
    for mode_etl in mode_etls:
        raw_query = re.sub(
            f" etl.{mode_etl}", f" mode_etl.{mode_etl}", raw_query, flags=re.IGNORECASE
        )

    return parse_redshift_to_snowflake(raw_query)


def update_datasource_and_sql_syntax(query):
    snowflake_query = {
        "query": {
            "data_source_id": SNOWFLAKE_DS_ID,
            "name": query["name"],
            "raw_query": update_query_sql_syntax(query["raw_query"]),
        }
    }
    mode_api(
        query["_forms"]["edit"]["method"],
        query["_forms"]["edit"]["action"],
        {"json": snowflake_query},
    )


def update_chart_capitalization(query):
    # capitalize attrs in latest query run view
    runs = mode_api("get", query["_links"]["query_runs"]["href"])
    last_run = runs["query_runs"][0]

    last_run_view_attrs = mode_api(
        "get", f'{last_run["_links"]["self"]["href"]}/view/attrs'
    )
    for attr in last_run_view_attrs["attrs"]:
        attr["formula_source"] = attr["formula_source"].upper()
        attr["name"] = attr["name"].upper()
        capitalized_attr = {"attr": attr}

        mode_api(
            "patch",
            f'{last_run["_links"]["self"]["href"]}/view/attrs/{attr["token"]}',
            {"json": capitalized_attr},
        )

    charts = mode_api("get", query["_links"]["charts"]["href"])
    for chart in charts["charts"]:
        snowflake_chart = {
            "chart": {
                "view": json.dumps(capitalize_view_fields(chart["view"])),
                "view_vegas": json.dumps(capitalize_view_fields(chart["view_vegas"])),
                "view_version": chart["view_version"],
                "color_palette_token": chart["color_palette_token"],
            }
        }

        mode_api(
            chart["_forms"]["edit"]["method"],
            chart["_forms"]["edit"]["action"],
            {"json": snowflake_chart},
        )


def update_filter_capitalization(report_token):
    report_filters = mode_api("get", f"/reports/{report_token}/filters")
    for report_filter in report_filters["report_filters"]:
        report_filter["name"] = report_filter["name"].upper()
        report_filter["formula"] = report_filter["formula"].upper()
        capitalized_report_filter = {"report_filter": report_filter}

        mode_api(
            report_filter["_forms"]["edit"]["method"],
            report_filter["_forms"]["edit"]["action"],
            {"json": capitalized_report_filter},
        )


def move_to_snowflake(report_token):
    logger.info(f"==== moving {report_token} to snowflake")
    update_filter_capitalization(report_token)

    report_queries = mode_api("get", f"/reports/{report_token}/queries")
    for query in report_queries["queries"]:
        if query["data_source_id"] == REDSHIFT_DS_ID:
            try:
                update_chart_capitalization(query)
            except Exception as e:
                logger.info(
                    "Cannot update chart capitalization, is there a table view in the report?"
                )
            update_datasource_and_sql_syntax(query)


def run_report(report_token):
    logger.info(f"==== running report {report_token}")
    run_report_response = mode_api("post", f"/reports/{report_token}/runs")
    logger.info("waiting for report run to finish...")

    while True:
        time.sleep(10)
        run = mode_api(
            "get", f"/reports/{report_token}/runs/{run_report_response['token']}"
        )
        run_state = run["state"]
        if run_state in ["succeeded", "cancelled", "failed"]:
            logger.info(f"report run {run_state}!")
            break
        else:
            logger.info(f"... {run_state}")


def sync_to_github(report_token, report_name, reason):
    logger.info(f"==== syncing report {report_token} to github ({reason})")

    sync_body = {"commit_message": f"sync '{report_name}' {reason}"}

    mode_api("patch", f"/reports/{report_token}/sync_to_github", {"json": sync_body})


def do_clone_to_snowflake(report_token_to_clone):
    cloned_report_token = clone_report(report_token_to_clone, "[SNOWFLAKE]")
    move_to_snowflake(cloned_report_token)
    run_report(cloned_report_token)
    logger.info("Clone complete!")
    logger.info(
        f"Original report: https://app.mode.com/<your-organization>/reports/{report_token_to_clone}"
    )
    logger.info(
        f"Cloned report: https://app.mode.com/<your-organization>/reports/{cloned_report_token}"
    )
    logger.info(
        f"Underlying snowflake tables parity summary: https://app.mode.com/<your-organization>/reports/<report-path>?param_report_token={cloned_report_token}"
    )


def do_backup_redshift(report_token):
    cloned_report_token = clone_report(report_token, "[REDSHIFT]")
    logger.info(
        f"View the backed up report at https://app.mode.com/<your-organization>/reports/{cloned_report_token}"
    )


def do_update_inplace(report_token):
    report = mode_api("get", f"/reports/{report_token}")
    sync_to_github(report_token, report["name"], "pre-migration")

    move_to_snowflake(report_token)
    run_report(report_token)

    sync_to_github(report_token, report["name"], "post-migration")

    logger.info(
        f"View the updated report at https://app.mode.com/<your-organization>/reports/{report_token}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--clone-to-snowflake",
        help="Clone the provided report token to a new report, and migrate to snowflake",
    )
    parser.add_argument(
        "--backup-redshift",
        help="Clone the provided report token, and rename to be a redshift backup",
    )
    parser.add_argument(
        "--update-inplace", help="Migrate the provided report token to snowflake"
    )
    parser.add_argument(
        "--backup-and-update",
        help="Backup the provided report token, and migrate the original to snowflake",
    )
    args = vars(parser.parse_args())

    if args["clone_to_snowflake"]:
        report_token_to_clone = args["clone_to_snowflake"]
        do_clone_to_snowflake(report_token_to_clone)
    elif args["backup_redshift"]:
        report_token_to_clone = args["backup_redshift"]
        do_backup_redshift(report_token_to_clone)
    elif args["backup_and_update"]:
        report_to_update = args["backup_and_update"]
        do_backup_redshift(report_to_update)
        do_update_inplace(report_to_update)
    elif args["update_inplace"]:
        report_to_update = args["update_inplace"]
        confirmation = "update this report in-place"
        answer = input(
            f"Are you sure you want to update this report in-place?  If it has been safely backed up and you want to proceed, enter '{confirmation}': "
        )
        if answer != confirmation:
            sys.exit("Confirmation message not received, exiting")
        do_update_inplace(report_to_update)


if __name__ == "__main__":
    main()
