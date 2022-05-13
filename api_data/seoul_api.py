import requests

from secret import DATA_SEOUL_API_KEY
from utils import DEFAULT_HEADERS, YEAR

SEOUL_URL = "http://openAPI.seoul.go.kr:8088/{}/{}/{}/{}/{}"


def get_openapi_seoul_data(service_key, start, end, type="json", year=YEAR, sub_info=""):
    req_url = SEOUL_URL.format(DATA_SEOUL_API_KEY, type, service_key, start, end)
    if sub_info:  # 여기에 year값을 넣어야 함
        req_url += f"/{sub_info}"
    res = requests.get(url=req_url, headers=DEFAULT_HEADERS)

    if res.status_code != 200:
        raise Exception(f"[{res.status_code}] api requests fails")

    if type == "json":
        data = res.json()

    raw_data = data.get(service_key, {})
    api_message = raw_data["RESULT"]
    if api_message["CODE"] != "INFO-000":
        raise Exception(f"[{api_message['CODE']}] {api_message['MESSAGE']}")

    total_count = raw_data.get("list_total_count", -1)
    result = raw_data.get("row", [])

    return {"count": total_count, "data": result}
