#!/bin/env python
"""
When retiring a node from ES gracefully, you should exclude that node from shard/index allocation.
you should do this via the api:

```
curl -XPUT http://elk-elasticsearch.service.prod.consul:9200/_cluster/settings -H 'Content-Type: application/json' -d '{  "transient" :{
      "cluster.routing.allocation.exclude._ip" : "10.128.230.159"
   }
}';echo
```

During the de-allocation of shards, ES will move indexes and shards about to ensure it complies with your outlined polices of shards + replicas.
This scrip will let you watch the process, so you know when the node is ready(SAFE) to be decommissioned/terminated.

After you terminate the NODE, you should remove the `exclude_ip` filter, so that if a new node comes up, with a matching IP, the new node will allocate shards and indexes.

curl -XPUT http://elk-elasticsearch.service.prod.consul:9200/_cluster/settings -H 'Content-Type: application/json' -d '{  "transient" :{
      "cluster.routing.allocation.exclude._ip" : ""
   }
}';echo



"""
import requests
import json
import sys
import time
from datetime import datetime

RETIRING_NODE = "10.128.230.159"
API_HOST = "http://elk-elasticsearch.service.prod.consul:9200"
HEALTH = "/_cluster/health"
SHARDS = "/_cat/shards?format=json"
STATS = "/_cluster/stats?human&pretty"
RET_NODE_STAT = "/_nodes/" + RETIRING_NODE + "/stats/indices?pretty‌"


def prettyfy_json(json_object):
    try:
        return json.dumps(json_object, indent=4, sort_keys=True)
    except Exception as e:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
            type(e).__name__,
            e,
        )


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
    try:
        relocating_shards = client(SHARDS, API_HOST)
        return relocating_shards['relocating_shards']
    except Exception as e:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
            type(e).__name__,
            e,
        )


def relocating_index(shards_json=shards_json):
    try:
        shard_index = []
        for shard in shards_json:
            if shard['state'] == "RELOCATING":
                shard_index.append(shard['index'])
                return shard_index
            else:
                pass
    except Exception as e:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
            type(e).__name__,
            e,
        )


def get_health(key_name):
    try:
        health_value = client(HEALTH, API_HOST)
        return health_value[key_name]
    except Exception as e:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
            type(e).__name__,
            e,
        )


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
    try:
        doc_count = client(PATH, API_HOST)['nodes']
        doc_count = get_arb_value(doc_count)['indices']['docs']['count']
        return doc_count
    except Exception as e:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
            type(e).__name__,
            e,
        )


def health_average(start_time):
    """
    build a list of important ES stats while decommissioning a node.
    then we will check each value if it is 0, because 0 means all unsafe
    operations have completed.
    then we loop, with a two second wait.
    example output:
    retiring node: 10.128.230.159 relocating: 28 initializing: 0 unassigned: 0 retiring_node_count: 878941271
    retiring node: 10.128.230.159 relocating: 28 initializing: 0 unassigned: 0 retiring_node_count: 878941468
    once all reaches 0, you are ready to kill that node.
    """
    try:
        loop_start = time.time()
        health = [
            get_health("relocating_shards"),
            get_health("initializing_shards"),
            get_health("unassigned_shards"),
            get_doc_count(RET_NODE_STAT)
        ]
        for stat in health:
            while stat in health != 0:
                time.sleep(2.0 - ((time.time() - loop_start) % 2.0))
                relocating = get_health("relocating_shards")
                initializing = get_health("initializing_shards")
                unassigned = get_health("unassigned_shards")
                doc_count = get_doc_count(RET_NODE_STAT)
                print("retiring node:", RETIRING_NODE, "relocating:",
                      relocating, "initializing:", initializing, "unassigned:",
                      unassigned, "retiring_node_count:", doc_count)
    except Exception as e:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
            type(e).__name__,
            e,
        )


def main():
    start_time = datetime.now()
    health_average(start_time)


main()
