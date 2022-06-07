import json
import os

import requests
from capd1.settings import MEDIA_URL

from main.models import Result
from main.serializers import ResultsSerializer
from main.utils import CONVERT_CODE


def show_result(obj):
    year = obj.year
    quarter = obj.quarter
    signgu_code = obj.signgu_code
    adstrd_cd = obj.adstrd_cd
    result_list = []

    file_name = f"{MEDIA_URL}{year}_{quarter}_report.json"
    results = read_big_file(file_name, str(adstrd_cd))
    for res in results:
        rank = res.get("RANK", -1)
        if rank < 100 and rank != -1:
            stage = 5
        elif rank < 300:
            stage = 4
        elif rank < 500:
            stage = 3
        elif rank < 700:
            stage = 2
        else:
            stage = 1

        obj = Result.objects.create(
            requests_id=obj.id,
            signgu_code=str(signgu_code),
            adstrd_cd=str(adstrd_cd),
            rank=rank,
            stage=stage,
        )
        result_list.append(obj)

    for funcs in [1, 2, 3, 4, 5, 6, 7, 8, 10, 14, 15]:
        file_name = f"{MEDIA_URL}{funcs}_{year}_{quarter}_report.json"
        func_res = file_read(file_name, adstrd_cd)

        for func_r in func_res:
            rank = res.get(f"RANK{funcs}", -1)
            if rank < 100 and rank != -1:
                stage = 5
            elif rank < 300:
                stage = 4
            elif rank < 500:
                stage = 3
            elif rank < 700:
                stage = 2
            else:
                stage = 1
            trdar_cd = func_r.get("TRDAR_CD")
            if trdar_cd is None:
                continue
            obj = Result.objects.create(
                requests_id=obj.id,
                signgu_code=str(signgu_code),
                adstrd_cd=str(adstrd_cd),
                trdar_cd=str(trdar_cd),
                rank=rank,
                rank_func=funcs,
                stage=stage,
            )
            result_list.append(obj)

    api_data = get_nemo_api(str(signgu_code), str(adstrd_cd))

    return result_list, api_data


def read_big_file(file_path, adstrd_cd):
    if not os.path.exists(file_path):
        return []
    res_list = []
    with open(file_path, "r") as file:
        data = json.load(file)
        for d in data:
            if d.get("ADSTRD_CD") == adstrd_cd:
                res_list.append(d)

    return res_list


def file_read(file_path, adstrd_cd):
    if not os.path.exists(file_path):
        return []

    res_list = []
    with open(file_path, "r") as file:
        data = json.load(file)
        for d in data:
            if d.get("ADSTRD_CD") == adstrd_cd:
                res_list.append(d)

    return res_list


def get_submunicipality_code(municipality_code):
    res = requests.get(f"https://www.nemoapp.kr/api/region/submunicipalities/{municipality_code}")
    if res.status_code != 200:
        raise Exception(f"[{res.status_code}] api requests fails")

    b_dong_list = res.json()

    if not b_dong_list:
        raise Exception("wrong municipality_code")

    return b_dong_list


def get_nemo_api(municipality_code, adstrd_cd):
    submunicipality_infos = get_submunicipality_code(municipality_code)
    res_list = list()
    for submunicipality_info in submunicipality_infos:
        code = submunicipality_info.get("code")
        if code == adstrd_cd:
            center = submunicipality_info.get("center")

            page_idx = 0
            res_list = list()
            while True:
                res = get_product(code, center["longitude"], center["latitude"], page_idx)

                if not res:
                    break

                res_list.extend(res)

                if page_idx == 5:
                    break
                page_idx += 1
    return res_list


def get_product(region, lng, lat, page=0, zoom=15):
    """
    매물 정보를 가져오는 함수
    """
    headers = {
        "authority": "www.nemoapp.kr",
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "dnt": "1",
        "referer": "https://www.nemoapp.kr/Search?ArticleType=1&PageIndex=0&SWLng=126.97309390890551&SWLat=37.47424501269658&NELng=127.02601325584673&NELat=37.50761852998374&Zoom=15&mode=1&category=1&list=true&articleId=&dataType=",
        "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    params = {
        "Radius": "",
        "Latitude": "",
        "Longitude": "",
        "Building": "",
        "PageSize": "",
        "SortBy": "",
        "DepositMin": "",
        "DepositMax": "",
        "MRentMin": "",
        "MRentMax": "",
        "SaleMin": "",
        "SaleMax": "",
        "Premium": "",
        "PremiumMin": "",
        "PremiumMax": "",
        "DealType": "",
        "ArticleType": "1",
        "BuildingType": "",
        "PriceType": "",
        "SizeMin": "",
        "SizeMax": "",
        "MFeeMin": "",
        "MFeeMax": "",
        "Floor": "",
        "IsAllFloors": "",
        "Parking": "",
        "ParkingSlotMin": "",
        "ParkingSlotMax": "",
        "Interior": "",
        "Elevator": "",
        "IndependentSpaceCount": "",
        "Toilet": "",
        "BYearMin": "",
        "BYearMax": "",
        "RoofTop": "",
        "Terrace": "",
        "PantryRoom": "",
        "AirConditioner": "",
        "VR": "",
        "OfficeShare": "",
        "ShopInShop": "",
        "OpenLateNight": "",
        "Remodeling": "",
        "AddSpaceOffer": "",
        "BusinessField": "",
        "IsExclusive": "",
        "AgentId": "",
        "UserId": "",
        "PageIndex": page,
        "Region": region,
        "Subway": "",
        "StoreTrade": "",
        "CompletedOnly": "",
        "LBiz": "",
        "MBiz": "",
        "InitialExpMin": "",
        "InitialExpMax": "",
        "IsCommercialDistrictUnknown": "",
        "IsCommercialDistrictSubway": "",
        "IsCommercialDistrictUniversity": "",
        "IsCommercialDistrictOffice": "",
        "IsCommercialDistrictResidential": "",
        "IsCommercialDistrictDowntown": "",
        "IsCommercialDistrictSuburbs": "",
        "MoveInDate": "",
        "HeatingType": "",
        "SWLng": lng,
        "SWLat": lat,
        "NELng": lng + 1,
        "NELat": lat + 1,
        "Zoom": zoom,
    }
    try:
        res = requests.get("https://www.nemoapp.kr/api/articles/search/", params=params, headers=headers)
        if res.status_code != 200:
            raise Exception(f"[{res.status_code}] api requests fails")

        raw_data = res.json()
        data = raw_data.get("items", [])

    except Exception as e:
        print(e.__str__())
        return False
    return data
