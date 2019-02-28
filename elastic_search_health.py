#!/bin/env python 

import requests
import json
import sys
import yaml
import os
import time
from datetime import datetime

API_URL = "http://elk-elasticsearch.service.prod.consul:9200"
HEALTH = "/_cluster/health"
SHARDS = "/_cat/shards?format=json"
#http://elk-elasticsearch.service.prod.consul:9200/_cat/shards?format=json


def prettyfy_json(json_object):
    return json.dumps(json_object, indent=4, sort_keys=True)


def client(API_URL, PATH):
    try:
        URL = API_URL + PATH
        get_state = requests.get(
            URL,
            verify=False,
        )
        state = get_state.json()
        return state
    except Exception as e:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
            type(e).__name__,
            e,
        )

def the_average(lst): 
    return sum(lst) / len(lst)

def health_average(start_time, health_json):
    try:
        relocating = []
        initializing = []
        unassigned = []
        loop_start = time.time()
        count = 10
        """
        relocating_shards: 0,
        initializing_shards: 32,
        unassigned_shards: 233,
        """
        while count != 0:
            time.sleep(2.0 - ((time.time() - loop_start) % 2.0))
            count -= 1
            print("STARTED::", start_time, "NOW::", datetime.now(), count)
            print(health_json['relocating_shards'])
            print(health_json['initializing_shards'])
            print(health_json['unassigned_shards'])
            relocating.append(health_json['relocating_shards'])
            initializing.append(health_json['initializing_shards'])
            unassigned.append(health_json['unassigned_shards'])
        print("relocating AVG", round(the_average(relocating), 2))
        print("initializing AVG", round(the_average(initializing), 2))
        print("unassigned AVG", round(the_average(unassigned), 2))
        # print("number of samples", len(health))
        return round(the_average(health), 2)
    except Exception as e:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
            type(e).__name__,
            e,
        )

def relocating_index(shards_json):
    shard_index = []
    for shard in shards_json:
        # print(shard['index'])
        if shard['state'] == "RELOCATING":
            # print(prettyfy_json(shard['index']))
            shard_index.append(shard['index'])
            return shard_index
        else:
            pass


def check():
    start_time = datetime.now()
    # print(start_time)
    shards_json = client(API_URL, SHARDS)
    health_json = client(API_URL, HEALTH)
    # print(prettyfy_json(health))
    print(relocating_index(shards_json))
    while health_average(start_time, health_json) != 0:
        print("still relocating shards ::", relocating_index(shards_json), prettyfy_json(health_json['relocating_shards']), "started at:", start_time)


check()
