import logging
import requests
import json
from typing import Optional


def getAPIdata(claim_id: int) -> Optional[str]:
    """Function to get data from API"""
    response = None # API response
    result = None # function's result
    url = f"https://api.hashify.net/hash/md4/hex?value={claim_id}"
    logging.info(url)
    try:
        response = requests.get(url)   
    except Exception as e:
        logging.error(e)
    if response != None and response.status_code == 200:
        try:
            result = json.loads(response.text)["Digest"]
        except Exception as e:
            logging.error(e)
    else:
        logging.error(response.status_code)

    return result

