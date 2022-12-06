import logging
import time
from pprint import pformat
from typing import Any, Dict, Tuple

import requests

log = logging.getLogger()


class DatafoldApiError(Exception):
    pass


class DatafoldApi(object):

    DATADIFF_REQUEST_FIELDS = [
        # mandatory: must always be present
        "pk_columns",  # List[str]
        "data_source1_id",  # int
        "data_source2_id",  # int
        # Either tableN or queryN must be specified
        "table1",  # Optional[List[str]] = None
        "table2",  # Optional[List[str]] = None
        "query1",  # Optional[str] = None
        "query2",  # Optional[str] = None
        # Optional fields
        "time_column",  # Optional[str] = None
        "filter1",  # Optional[str] = None
        "filter2",  # Optional[str] = None
        "sampling_tolerance",  # Optional[float] = None
        "sampling_confidence",  # Optional[float] = None
        "exclude_columns",  # Optional[List[str]] = None
        "diff_tolerance_per_column",  # Optional[List[dict]] = None
    ]

    def __init__(
        self,
        *,
        api_key,
        datafold_datasource_id,
        base_url="https://app.datafold.com",
        **_
    ):
        """ Constructor for this API class

        Args:
            api_key: datafold api key
            datafold_datasource_id: datafold datasource id found on https://app.datafold.com/data_sources
            base_url: base url for the datafold api
        """

        self.api_key = api_key
        self.datafold_datasource_id = int(datafold_datasource_id)
        self.base_url = base_url

    def run_diff(
        self,
        table1,
        table2,
        query1,
        query2,
        pks,
        exclude_columns,
        filter,
        sampling_tolerance,
        sampling_confidence,
        diff_tolerances_per_column,
    ):
        if not (all((table1, table2)) or all((query1, query2))):
            raise DatafoldApiError(
                "Failed to submit diff. You need to provide either tables or query to run the diff on"
            )
        else:
            comparison_keys = (
                {"query1": query1, "query2": query2}
                if all((query1, query2))
                else {
                    "table1": [obj.upper() for obj in table1.split(".")],
                    "table2": [obj.upper() for obj in table2.split(".")],
                }
            )

        req = {
            "data_source1_id": self.datafold_datasource_id,
            "data_source2_id": self.datafold_datasource_id,
            "pk_columns": [pk.upper() for pk in pks],
            "exclude_columns": [col.upper() for col in exclude_columns]
            if exclude_columns
            else [],
            "filter1": filter,
            "filter2": filter,
            "sampling_tolerance": sampling_tolerance,
            "sampling_confidence": sampling_confidence,
            "diff_tolerances_per_column": diff_tolerances_per_column
            if diff_tolerances_per_column
            else [],
        }

        # Update req with keys that point to the data source. Table vs. query
        req.update(comparison_keys)

        url, res = self._run_diff(self.base_url, self.api_key, req)

        log.info("Datafold url: %s", url)
        log.info("Datafold result: %s", pformat(res))

        return url, res

    def _run_diff(self, base_url, api_key: str, req: Dict[str, Any]) -> Tuple[str, Any]:
        """Implementation borrowed from datafold

        Args:
            base_url: base url for the api call
            api_key: api key
            req: req object

        Returns:
            Tuple of datafold result url and the result payload
        """

        base_api_url = base_url + "/api/datadiffs"

        payload = {f: None for f in self.DATADIFF_REQUEST_FIELDS}
        payload.update(req)
        log.info("Request payload: %s", payload)
        headers = {"Authorization": "Key " + api_key}
        r = requests.post(url=base_api_url, json=payload, headers=headers)
        if r.status_code != 200:
            raise DatafoldApiError("Failed to submit diff: {}".format(r.text))
        response = r.json()
        log.info(response)

        job_id = response["id"]
        while True:
            job = requests.get(
                url="{}/{}?poll".format(base_api_url, job_id), headers=headers
            )

            log.info("Polling job result received: %s", job)

            task = job.json()
            subtasks_done = all(
                status in ("done", "failed")
                for status in task["result_statuses"].values()
            )
            if task["done"] is True and subtasks_done:
                log.info("Diff done!")
                break

            time.sleep(3)

        results_reply = requests.get(
            url="{}/{}/results_summary".format(base_api_url, job_id), headers=headers
        )
        results = results_reply.json()

        ui_url = base_url + "/datadiffs/" + str(task["id"])
        return ui_url, results
