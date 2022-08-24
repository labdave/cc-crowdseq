import random


def get_api_sleep(attempt):
    temp = 4 * 2 ** attempt
    return int(temp / 2) + random.randrange(0, temp/2)
