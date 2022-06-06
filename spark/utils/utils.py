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
    funcs = list(range(1, 16))
    funcs.remove(9)  # 9 번은 계산 함수임
    funcs.remove(12)  # 12 번은 14번 함수로 대체될 수 있음
    funcs.remove(13)  # 13 번은 14번 함수로 대체될 수 있음

    years = list(range(2018, 2023))
    quarters = list(range(1, 5))
    if func:
        funcs = func
    if year:
        years = [year]
    if quarter:
        quarters = [quarter]

    return years, quarters, funcs
