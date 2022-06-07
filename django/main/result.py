import json
import os

import requests
from capd1.settings import MEDIA_URL

from main.models import Result
from main.utils import CODE, CONVERT_CODE, CONVERT_HJD, FUNC_INFOS, INVERSE_CONVERT_HJD, NEARBY, SEOUL_MUNICIPALITY_CODE


def show_big_result(year, quarter, signgu_code, adstrd_cd):

    file_name = f"{MEDIA_URL}{year}_{quarter}_report.json"
    result_list = list()
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

    return result_list


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


def show_middle_result(obj_id, year, quarter, signgu_code, raw_adstrd_cd):
    average_rank = 0
    total = 0
    result_list = []
    already_done = []
    adstrd_cd = raw_adstrd_cd
    for funcs in [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 14, 15]:
        file_name = f"{MEDIA_URL}{funcs}_{year}_{quarter}_report.json"
        func_res = file_read(file_name, raw_adstrd_cd)

        # 해당 라인의 값을 가져온다.
        for fun_r in func_res:
            try:
                rank = fun_r.get(f"RANK{funcs}", -1)

                if rank < 100 and rank != -1:
                    stage = "⭐⭐⭐⭐⭐"
                elif rank < 300:
                    stage = "⭐⭐⭐⭐"
                elif rank < 500:
                    stage = "⭐⭐⭐"
                elif rank < 700:
                    stage = "⭐⭐"
                else:
                    stage = "⭐"

                trdar_cd = fun_r.get("TRDAR_CD")

                func_info = FUNC_INFOS[funcs]
                func_name = func_info["name"]
                if func_name not in already_done:
                    already_done.append(func_name)
                    func_url = func_info["url"]

                    try:
                        adstrd_cd_name = INVERSE_CONVERT_HJD[adstrd_cd]  # 11680545
                        adstrd_cd = find_f_code(adstrd_cd_name)  # 11680110

                    except:
                        adstrd_cd_name = find_f_n_code(signgu_code, adstrd_cd)

                    if adstrd_cd is None:
                        continue

                    print(func_name)
                    print(adstrd_cd_name)
                    print()

                    obj = Result.objects.create(
                        requests_id=obj_id,
                        signgu_code=SEOUL_MUNICIPALITY_CODE[signgu_code],
                        adstrd_cd=adstrd_cd_name,
                        trdar_cd=CONVERT_CODE.get(signgu_code, {}).get(adstrd_cd, {}).get(trdar_cd, "상권이 아닙니다"),
                        rank=rank,
                        func_name=func_name,
                        func_url=func_url,
                        stage=stage,
                    )

                    if rank != -1:
                        average_rank += rank
                    total += 1
                    result_list.append(obj)
            except:
                continue

    rank = 0
    if total != 0:
        rank = int(average_rank // total)

    return result_list, rank


def find_f_code(name):
    for v in CODE.values():
        for k2, v2 in v.items():
            if v2 == name:
                return k2


def find_f_n_code(sg, code):
    for k, v in CODE[sg].items():
        if k == code:
            return v


def show_result(obj, nearby=False):
    year = obj.year
    quarter = obj.quarter
    signgu_code = str(obj.signgu_code)  #
    f_adstrd_cd = str(obj.adstrd_cd)

    adstrd_cd_name = CODE.get(signgu_code, {}).get(f_adstrd_cd)
    adstrd_cd = CONVERT_HJD.get(adstrd_cd_name)  #  11500620
    result_list = []
    api_data = []
    rank = 0
    # show_big_result(year, quarter, signgu_code, adstrd_cd)

    if not nearby:
        api_data = get_nemo_api(signgu_code, f_adstrd_cd)
        result_list, rank = show_middle_result(obj.id, year, quarter, signgu_code, adstrd_cd)
    else:
        nearby_sigs = NEARBY[signgu_code]
        nearby_rank = 0
        nearby_total = 0

        for sg in nearby_sigs:
            sg_str = str(sg)
            for ad_name in list(CODE.get(sg_str, {}).values()):

                ad = CONVERT_HJD.get(ad_name)
                if ad is None:
                    continue

                res, rank = show_middle_result(obj.id, year, quarter, sg_str, ad)
                result_list.extend(res)
                nearby_rank += rank
                nearby_total += len(res)
        if nearby_total != 0:
            rank = int(nearby_rank // nearby_total)

    return result_list, rank, api_data


def file_read(file_path, adstrd_cd: str) -> list():
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

                if len(res_list) > 5:
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

    if len(data) > 5:
        data = data[:4]
    return data
