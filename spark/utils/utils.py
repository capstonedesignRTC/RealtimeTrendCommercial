# 서울특별시


def get_sys_args(ls: list):
    """
    시스템 변수 가져오는 함수

    1. 특정 년도의 결과값만 보고 싶을 경우 2018~2022(년) 사이의 값을 입력한다. 만약 -1을 입력하면 설정하지 않는다.
    2. 특정 분기의 결과값만 보고 싶을 경우 1~4(분기) 사이의 값을 입력한다. 만약 -1을 입력하거나 아무런 값을 입력하지 않는다면 설정하지 않는다.
    3. 특정 함수(들)의 결과값만 보고 싶을 경우 함수에 해당하는 번호를 입력한다. 만약 -1을 입력하면 설정하지 않는다.
    """
    res = dict(funcs=list())
    for idx, l in enumerate(ls):
        l = int(l)
        if l == -1:
            continue
        if idx == 0:
            res["year"] = l
        elif idx == 1:
            res["quarter"] = l
        else:
            res["funcs"].append(l)

    year, quarter, func = res.get("year", False), res.get("quarter", False), res.get("funcs", False)
    funcs = list(range(1, 17))
    years = list(range(2018, 2023))
    quarters = list(range(1, 5))
    if funcs:
        funcs = func
    if year:
        years = [year]
    if quarter:
        quarters = [quarter]

    return years, quarters, funcs


SEOUL_MUNICIPALITY_CODE = {
    "강남구": 11680,
    "강동구": 11740,
    "강북구": 11305,
    "강서구": 11500,
    "관악구": 11620,
    "광진구": 11215,
    "구로구": 11530,
    "금천구": 11545,
    "노원구": 11350,
    "도봉구": 11320,
    "동대문구": 11230,
    "동작구": 11590,
    "마포구": 11440,
    "서대문구": 11410,
    "서초구": 11650,
    "성동구": 11200,
    "성북구": 11290,
    "송파구": 11710,
    "양천구": 11470,
    "영등포구": 11560,
    "용산구": 11170,
    "은평구": 11380,
    "종로구": 11110,
    "중구": 11140,
    "중랑구": 11260,
}

