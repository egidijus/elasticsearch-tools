#!/bin/env python

import requests
import json
import sys
import yaml
import os
import time
from datetime import datetime

RETIRING_NODE = "10.128.230.159"
API_HOST = "http://elk-elasticsearch.service.prod.consul:9200"
HEALTH = "/_cluster/health"
SHARDS = "/_cat/shards?format=json"
STATS = "/_cluster/stats?human&pretty"
RET_NODE_STAT = "/_nodes/" + RETIRING_NODE + "/stats/indices?pretty‌"

# 'http://elk-elasticsearch.service.prod.consul:9200/_nodes/10.128.230.159/stats/indices?pretty‌'

#http://elk-elasticsearch.service.prod.consul:9200/_cat/shards?format=json


def prettyfy_json(json_object):
    return json.dumps(json_object, indent=4, sort_keys=True)


def client(PATH, API_HOST=API_HOST):
    """
    accepts a path for API and an API_HOST
    returns a json document from elasticsearch.
    """

    try:
        URL = API_HOST + PATH
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


shards_json = client(SHARDS, API_HOST)
health_json = client(HEALTH, API_HOST)


def get_relocating_shards():
    relocating_shards = client(SHARDS, API_HOST)
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
    health_value = client(HEALTH, API_HOST)
    # print(health_json)
    # print(health_value[key_name])
    return health_value[key_name]


def get_arb_value(dictionary):
    """
    because "/_nodes/" + RETIRING_NODE + "/stats/indices?pretty‌"
    or get_stats(RET_NODE_STAT)['nodes']
    or "/_nodes/" + RETIRING_NODE + "/stats/indices?pretty‌"['nodes']
    will return a dictionary with mystery key for the node ID, we want to
    get to the values, we simply iterate.
    """
    try:
        # print(next(iter(dictionary.values())))
        return next(iter(dictionary.values()))
    except StopIteration:
        # print(next(iter(dictionary.values())))
        print("NOTHING here captain")
        return []


def get_doc_count(PATH, API_HOST=API_HOST):
    """
    this should return just a count of documents from the key:
    ['indices']['docs']['count']

    """
    doc_count = client(PATH, API_HOST)['nodes']
    # print(stats)
    # print(stats)['nodes'][0]
    # return stats['indices']['docs']['count']
    doc_count = get_arb_value(doc_count)['indices']['docs']['count']
    # print(doc_count)
    # # print(prettyfy_json(get_arb_value(get_stats(RET_NODE_STAT)['nodes'])))
    return doc_count


def health_average(start_time):
    try:
        loop_start = time.time()
        count = 10
        """
        relocating_shards: 0,
        initializing_shards: 32,
        unassigned_shards: 233,
        """
        health = [
            get_health("relocating_shards"),
            get_health("initializing_shards"),
            get_health("unassigned_shards"),
            get_doc_count(RET_NODE_STAT)
        ]
        for stat in health:
            # print(stat)
            while stat in health != 0:
                time.sleep(2.0 - ((time.time() - loop_start) % 2.0))
                count -= 1
                relocating = get_health("relocating_shards")
                initializing = get_health("initializing_shards")
                unassigned = get_health("unassigned_shards")
                doc_count = get_doc_count(RET_NODE_STAT)
                # count = 10000
                print("retiring node:", RETIRING_NODE, "relocating:",
                      relocating, "initializing:", initializing, "unassigned:",
                      unassigned, "retiring_node_count:", doc_count)
                # return health = [relocating_avg, initializing_avg, unassigned_avg, count_avg]
    except Exception as e:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
            type(e).__name__,
            e,
        )


def check():
    start_time = datetime.now()
    # print(prettyfy_json(get_arb_value(get_stats(RET_NODE_STAT)['nodes'])))
    # print(get_doc_count(RET_NODE_STAT))

    # print(start_time)
    # shards_json = client(API_HOST, SHARDS)
    # health_json = client(API_HOST, HEALTH)
    # print(prettyfy_json(health))
    # print(relocating_index(shards_json))
    health_average(start_time)
    # while health_average(start_time, health_json) != 0:
    #     print("still relocating shards ::", relocating_index(shards_json),
    #           prettyfy_json(health_json['relocating_shards']), "started at:",
    #           start_time)


check()
