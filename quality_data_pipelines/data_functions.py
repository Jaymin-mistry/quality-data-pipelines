def convert_negative_to_positive(value):
    if value < 0:
        return abs(value)
    return value


def put_in_age_group(age: int) -> str:
    if age < 18:
        group = "0-18"
    elif 18 <= age < 30:
        group = "18-30"
    elif 30 <= age < 50:
        group = "30-50"
    elif 50 <= age < 70:
        group = "50-70"
    else:
        group = "over 70"

    return group
