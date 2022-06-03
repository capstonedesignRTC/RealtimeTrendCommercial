import json
import logging

import requests

from secret import DATA_SEOUL_API_KEY
from utils import DEFAULT_HEADERS

SEOUL_URL = "http://openAPI.seoul.go.kr:8088/{}/{}/{}/{}/{}"


def get_all_seoul_data(key, service_key, year):
    start, limit = 0, 900
    while True:
        if start > 7000:
            break
        if key == 6:  # 분기가 필요한 api
            for quarter in [1, 2, 3, 4]:
                yield get_openapi_seoul_data(
                    service_key=service_key, start=start, end=start + limit, year=year, quarter=quarter
                )
        else:
            yield get_openapi_seoul_data(
                service_key=service_key,
                start=start,
                end=start + limit,
                year=year,
            )
        start += limit


def get_openapi_seoul_data(service_key, start, end, type="json", year="", quarter=""):
    req_url = SEOUL_URL.format(DATA_SEOUL_API_KEY, type, service_key, start, end)
    if year:
        req_url += f"/{year}"
    if quarter:
        req_url += f"/{quarter}"
    res = requests.get(url=req_url, headers=DEFAULT_HEADERS)

    if res.status_code != 200:
        logging.error(f"[{res.status_code}] api requests fails")
        return {}
    try:
        if type == "json":
            data = res.json().get(service_key, {})

        status = data.get("RESULT", {})
        if not status:
            status = json.loads(res.text)["RESULT"]
            logging.error(f"[{status['CODE']}] {status['MESSAGE']}")
            return {}

        if status["CODE"] != "INFO-000":
            logging.error(f"[{status['CODE']}] {status['MESSAGE']}")
            return {}

        total_count = data.get("list_total_count", -1)
        result = data.get("row", [])

        res = {"count": total_count, "data": result}
        if year:
            res.update({"year": year})
        if quarter:
            res.update({"quarter": quarter})

        res.update({"page": int(start // 900) + 1})

        return res
    except Exception as e:
        logging.error(e.__str__())
        return {}
