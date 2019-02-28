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
STATS = "/_cluster/stats?human&pretty"

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
        # print(state)
        return state
    except Exception as e:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
            type(e).__name__,
            e,
        )


shards_json = client(API_URL, SHARDS)
health_json = client(API_URL, HEALTH)


def get_relocating_shards():
    relocating_shards = client(API_URL, SHARDS)
    print(shards_json)
    return relocating_shards['relocating_shards']


def relocating_index(shards_json=shards_json):
    shard_index = []
    for shard in shards_json:
        # print(shard['index'])
        if shard['state'] == "RELOCATING":
            # print(prettyfy_json(shard['index']))
            shard_index.append(shard['index'])
            return shard_index
        else:
            pass


def get_health(key_name):
    health_value = client(API_URL, HEALTH)
    # print(health_json)
    # print(health_value[key_name])
    return health_value[key_name]


def get_stats():
    stats = client(API_URL, STATS)
    return stats


def the_average(lst):
    return sum(lst) / len(lst)


def health_average(start_time):
    try:
        al = {
            "relocating_shards": [],
            "initializing_shards": [],
            "unassigned_shards": [],
            "relocating_index": [],
            "documents": []
        }
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
            al['relocating_shards'].append(get_health("relocating_shards"))
            health['initializing_shards'].append(
                get_health("initializing_shards"))
            al['unassigned_shards'].append(get_health("unassigned_shards"))
            al['documents'].append(get_stats()['indices']['docs']['count'])
            relocating_avg = round(the_average(al['relocating_shards']), 2)
            initializing_avg = round(the_average(al['initializing_shards']), 2)
            count_avg = round(the_average(al['documents']), 2)
            unassigned_avg = round(the_average(al['unassigned_shards']), 2)

            print("relocating:", relocating_avg, "initializing:",
                  initializing_avg, "unassigned:", unassigned_avg, "count:",
                  count_avg)
            return health = [relocating_avg, initializing_avg, unassigned_avg, count_avg]
            # print(prettyfy_json(get_stats()['indices']['segments']['count']))
            # print(prettyfy_json(get_stats()['indices']['docs']['count']))

    except Exception as e:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
            type(e).__name__,
            e,
        )


def check():
    start_time = datetime.now()
    # print(start_time)
    # shards_json = client(API_URL, SHARDS)
    # health_json = client(API_URL, HEALTH)
    # print(prettyfy_json(health))
    # print(relocating_index(shards_json))
    health_average(start_time)
    # while health_average(start_time, health_json) != 0:
    #     print("still relocating shards ::", relocating_index(shards_json),
    #           prettyfy_json(health_json['relocating_shards']), "started at:",
    #           start_time)


check()
