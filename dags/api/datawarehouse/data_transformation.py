from datetime import timedelta

def parse_duration(duration_str: str) -> timedelta:
    if not duration_str.startswith("P"):
        raise ValueError("Invalid ISO 8601 duration")

    duration_str = duration_str.replace("P", "").replace("T", "")

    components = ["D", "H", "M", "S"]
    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component, 1)
            if not value.isdigit():
                raise ValueError("Invalid ISO 8601 duration")
            values[component] = int(value)

    print(values["M"])
    return timedelta(
        days=values["D"],
        hours=values["H"],
        minutes=values["M"],
        seconds=values["S"],
    )

def transform_data(row):
    duration_td = parse_duration(row['Duration'])
    total_sec = duration_td.total_seconds()
    
    row['Duration'] = total_sec
    row['Video_Type'] = 'Shorts' if total_sec <= 60 else 'Normal'

    return row