import requests

from secret import DATA_SEOUL_API_KEY
from utils import DEFAULT_HEADERS, YEAR

SEOUL_URL = "http://openAPI.seoul.go.kr:8088/{}/{}/{}/{}/{}"


def get_openapi_seoul_data(service_key, start, end, type="json", year="", sub_info=""):
    req_url = SEOUL_URL.format(DATA_SEOUL_API_KEY, type, service_key, start, end)
    # if year:  # 여기에 year값을 넣어야 함
    #     req_url += f"/{year}"
    res = requests.get(url=req_url, headers=DEFAULT_HEADERS)

    if res.status_code != 200:
        raise Exception(f"[{res.status_code}] api requests fails")
    try:
        if type == "json":
            data = res.json().get(service_key, {})

        status = data["RESULT"]
        if status["CODE"] != "INFO-000":
            raise Exception(f"[{status['CODE']}] {status['MESSAGE']}")

        total_count = data.get("list_total_count", -1)
        result = data.get("row", [])

        return {"count": total_count, "data": result}
    except Exception as e:
        raise Exception(e)