import logging

import requests


def get_submunicipality_code(municipality_code):
    """
    법정동 코드로 행정동 코드 가져오는 함수
    `SEOUL_MUNICIPALITY_CODE` 변수에 정리 완료
    """
    res = requests.get(f"https://www.nemoapp.kr/api/region/submunicipalities/{municipality_code}")
    if res.status_code != 200:
        raise Exception(f"[{res.status_code}] api requests fails")

    b_dong_list = res.json()

    if not b_dong_list:
        raise Exception("wrong municipality_code")

    return b_dong_list


def get_nemo_api(municipality_code):
    submunicipality_infos = get_submunicipality_code(municipality_code)
    for submunicipality_info in submunicipality_infos:
        code = submunicipality_info.get("code")
        center = submunicipality_info.get("center")

        page_idx = 0
        while True:
            res = get_product(code, center["longitude"], center["latitude"], page_idx)

            if not res.get("data", []):
                break

            yield res

            if page_idx == 5:
                break
            page_idx += 1


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
        logging.error(e.__str__())
    return {"region": region, "data": data}
