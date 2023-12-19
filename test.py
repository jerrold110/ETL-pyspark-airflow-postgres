from datetime import datetime

now = datetime.now()#datetime(2023, 11, 30, 0, 0, 0)


def date_filepath(ts):
    year = ts.strftime("%Y")
    month = ts.strftime("%m")
    day = ts.strftime("%d")

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")
    minute = now.strftime("%M")
    second = now.strftime("%S")

    return f'{year}-{month}-{day}-{hour}-{minute}-{second}'

date_filepath(now)

