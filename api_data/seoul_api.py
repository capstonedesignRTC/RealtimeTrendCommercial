import logging

import requests

from secret import DATA_SEOUL_API_KEY
from utils import DEFAULT_HEADERS, YEAR

SEOUL_URL = "http://openAPI.seoul.go.kr:8088/{}/{}/{}/{}/{}"


def get_openapi_seoul_data(service_key, start, end, type="json", year="", sub_info=""):
    req_url = SEOUL_URL.format(DATA_SEOUL_API_KEY, type, service_key, start, end)
    if year:
        req_url += f"/{year}"
    res = requests.get(url=req_url, headers=DEFAULT_HEADERS)

    if res.status_code != 200:
        logging.error(f"[{res.status_code}] api requests fails")
        return {}
    try:
        if type == "json":
            data = res.json().get(service_key, {})

        status = data["RESULT"]
        if status["CODE"] != "INFO-000":
            logging.error(f"[{status['CODE']}] {status['MESSAGE']}")
            return {}

        total_count = data.get("list_total_count", -1)
        result = data.get("row", [])

        res = {"count": total_count, "data": result}
        if year:
            res.update({"year": year})
        return res
    except Exception as e:
        logging.error(e.__str__())
        return {}
