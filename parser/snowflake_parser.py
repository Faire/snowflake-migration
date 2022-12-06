import argparse
import logging.config
import re

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def parse_redshift_to_snowflake(sql):
    if not sql:
        return sql

    for parser in parsers:
        if not sql:
            return sql
        try:
            sql = parser(sql)
        except Exception as e:
            logger.error(sql)
            logger.exception(e)
        logger.debug(f"Partial parsed sql: {sql}")
    logger.info(f"Parsed snowflake sql: {sql}")
    return sql


def parse_encoding(sql):
    """Remove encodings from DDL statements"""
    pattern = r"[\s]encode .*,"
    replace = r","
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)

    pattern = r"[\s]encode .*"
    replace = r""
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)

    pattern = r"[\s]timestamp without time zone"
    replace = r" timestamp_ntz(9)"
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)

    return sql


def parse_dist_sort(sql):
    """Remove diststyle and sortkeys from DDL statements"""
    pattern = r"[\s]diststyle (all|even|auto|key)"
    replace = r""
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)

    pattern = r"distkey.*\)"
    replace = r""
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE + re.MULTILINE + re.DOTALL)

    pattern = r"[\s]diststyle.*\)"
    replace = r""
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)

    pattern = r"((compound|interleaved) )?sortkey\s*\([^\(]*\)"
    replace = r""
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE + re.MULTILINE + re.DOTALL)

    # sometimes distkey is defined in column attributes
    pattern = r"distkey"
    replace = r""
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE + re.MULTILINE + re.DOTALL)
    return sql


def parse_bool(sql):
    """Convert bool to boolean"""
    pattern = r" bool[\s]*([,\n])"
    replace = r" boolean\1"
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)

    pattern = r"::[\s]*bool "
    replace = r":: boolean "
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)

    return sql


def parse_int(sql):
    """Convert int to snowflake data type"""
    int_patterns = [r" integer", r" int2", r" int4", r" int8", r" bigint", r" smallint"]
    for pattern in int_patterns:
        replace = r" int"
        sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)

    return sql


def parse_varchar(sql):
    """Convert int to snowflake data type"""
    pattern = r" varchar\([0-9]+\)"
    replace = r" VARCHAR"
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)

    return sql


def parse_trailing_whitespace(sql):
    """Remove trailing whitespace, newline, line terminators"""
    return sql.rstrip("\n\r").rstrip()


def parse_search_path(sql):
    """Convert set search_path"""
    pattern = r"set search_path to "
    replace = r"use schema "
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)
    return sql


def parse_analyze(sql):
    """Remove all analyze statements"""
    pattern = r"analyze [\.\w\s]*;"
    replace = r""
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)
    return sql


def parse_wlm_query_slot_count(sql):
    pattern = r"set wlm_query_slot_count to [\d]+;"
    replace = r""
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)
    return sql


def parse_ctas(sql):
    """Convert create table as statements"""
    pattern = r"create table (.*) \([\s]*like[\s]*(.*)[\s]*\)"
    replace = r"create table \1 like \2"
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE + re.MULTILINE)
    return sql


def parse_atrt(sql):
    """Convert alter table rename to statements"""

    pattern = r"alter table (\w+)\.(\w+) rename to (\w+)"
    replace = r"alter table \1.\2 rename to \1.\3"
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE + re.MULTILINE)
    return sql


def parse_convert_timezone_utc(sql):
    """Convert utc time zone"""

    pattern = r"convert_timezone\('utc'"
    replace = r"convert_timezone('UTC'"
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE + re.MULTILINE)
    return sql


def parse_convert_timezone_pacific(sql):
    """Convert pacific time zones"""

    pattern = r"convert_timezone\(('pst'|'pdt')"
    replace = r"convert_timezone('America/Los_Angeles'"
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE + re.MULTILINE)
    return sql


def parse_convert_date_trunc_weeks(sql):
    """Convert date_trunc with weeks"""

    pattern = r"date_trunc\('weeks'"
    replace = r"date_trunc('week'"
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE + re.MULTILINE)
    return sql


def parse_swap_methods(sql):
    """Simple 1:1 swaps of method names. For example BOOL_OR() -> BOOLOR_AGG()"""
    swaps = {
        "BOOL_OR": "BOOLOR_AGG",
        "BOOL_AND": "BOOLAND_AGG",
        " ~ '": " regexp '",
        "BTRIM": "TRIM",
        "CHAR_LENGTH": "LEN",
        "DATEPART": "DATE_PART",
        "DATE_DIFF": "DATEDIFF",
        "DATE_ADD\(": "DATEADD(",
        "(DATEDIFF|DATE_DIFF)(\('weeks',)(.*)": r"DATEDIFF('week', \3",
        "FROM_UNIXTIME": "TO_TIMESTAMP",
        r"JSON_EXTRACT_PATH_TEXT\(([\w\.]*), '(\w*)', true\)": r"\1:\2",
        "IS FALSE": " = FALSE",
        "IS TRUE": " = TRUE",
        "NVL": "COALESCE",
        "SYSDATE": "CURRENT_TIMESTAMP",
        "CEILING\(": "CEIL(",
        "ISNULL\(": "NVL(",
    }
    for orig, new in swaps.items():
        sql = re.sub(orig, new, sql, flags=re.IGNORECASE)
    return sql


def parse_tmp(sql):
    """Convert tmp stuff"""
    pattern = r"production\.user_sessions_view"
    replace = r"production.user_sessions"
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)

    pattern = r"rating__bigint"
    replace = r"rating"
    sql = re.sub(pattern, replace, sql, flags=re.IGNORECASE)
    return sql


parsers = [
    parse_encoding,
    parse_dist_sort,
    parse_bool,
    parse_int,
    parse_varchar,
    parse_trailing_whitespace,
    parse_swap_methods,
    parse_search_path,
    parse_analyze,
    parse_ctas,
    parse_atrt,
    parse_tmp,
    parse_wlm_query_slot_count,
    parse_convert_timezone_utc,
    parse_convert_timezone_pacific,
    parse_convert_date_trunc_weeks,
]


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "sql_path",
        help="A sql file that you want parse into snowflake syntax",
        type=str,
    )

    args = arg_parser.parse_args()

    with open(args.sql_path) as f:
        parsed_sql = parse_redshift_to_snowflake(f.read())
