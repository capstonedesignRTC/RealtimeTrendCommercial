import json
import logging

import requests

from secret import DATA_GO_KR_DE, DATA_GO_KR_EN
from utils import DATA_GO_KR_API_KEYS

DATA_GOV_URL = "https://api.odcloud.kr/api/{}"


# def get_all_data_go_data(key, service_key):
#     page, limit = 1, 20
#     while True:
#         if page == 100:
#             break

#         yield get_odcloud_gov_data(
#             service_key=service_key,
#             page=start,
#             end=start + limit,
#             year=year,
#         )
#         start += limit


def two_get_odcloud_gov_data(service_key, key):

    req_url = DATA_GOV_URL.format(DATA_GO_KR_API_KEYS.get(service_key))
    headers = {"accept": "application/json", "Authorization": DATA_GO_KR_EN}
    params = {"serviceKey": DATA_GO_KR_DE, "key": key, "type": "json"}

    res = requests.get(url=req_url, params=params, headers=headers)
    try:
        if res.status_code != 200:
            raise Exception(f"[{res.status_code}] api requests fails")

        data = res.json()
        total_count = data.get("currentCount", -1)
        result = data.get("data", [])
        return {"count": total_count, "data": result}
    except Exception as e:
        logging.error(e.__str__())
        return {}


def one_get_odcloud_gov_data(service_key, page, size, type="json", sub_params={}):

    req_url = DATA_GOV_URL.format(DATA_GO_KR_API_KEYS.get(service_key))
    headers = {"accept": "application/json", "Authorization": DATA_GO_KR_EN}

    params = {"serviceKey": DATA_GO_KR_DE, "type": "json"}
    if sub_params:
        params.update({**sub_params})
    else:
        params.update({"page": page, "perPage": size})

    # if type == "XML":
    #     params.update({"returnType": type, "type": type})

    res = requests.get(url=req_url, params=params, headers=headers)
    try:
        if res.status_code != 200:
            raise Exception(f"[{res.status_code}] api requests fails")

        data = res.json()
        total_count = data.get("currentCount", -1)
        result = data.get("data", [])
        return {"count": total_count, "data": result}
    except Exception as e:
        logging.error(e.__str__())
        return {}

