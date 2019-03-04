from __future__ import print_function

import argparse
import random
import json

import requests

__author__ = 'Alex Zaitsev'
__copyright__ = 'Alex Zaitsev, TargetProcess 2016'


class ES(object):

    def __init__(self, es_ip='127.0.0.1', es_port=9200, nodes=['127.0.0.1']):
        self.ip = es_ip
        self.port = es_port
        self.nodes = nodes

    def get_unassigned_indices_and_shards(self):
        """Extract name and numbers of shard and indices from ES."""
        unassigned = {}
        es_shards_info = requests.get('http://{0}:{1}/_cat/shards?format=json'.format(
            self.ip, self.port))
        for item in es_shards_info.json():
            if item.get('state') == 'UNASSIGNED':
                index = item.get('index')
                shard = item.get('shard')
                if not unassigned.get(index):
                    unassigned[index] = []
                if shard not in unassigned[index]:
                    unassigned[index].append(shard)
        return unassigned

    def allocate_shard(self, index, shard):
        """Assign shard to node."""
        node = random.choice(self.nodes)
        payload = {
            'commands': [
                {'allocate': {
                    'index': index,
                    'shard': shard,
                    'node': node,
                    'allow_primary': 1}}
            ]
        }
        print('Allocating {0} shard {1} to node {2}... '.format(index, shard, node), end='')
        res = requests.post('http://{0}:{1}/_cluster/reroute'.format(
            self.ip, self.port), data=json.dumps(payload))
        if res.status_code == 200:
            print('success!')
        else:
            print('error :(')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Plunger params.')
    parser.add_argument('--host', dest='host', default='127.0.0.1', help='elasticsearch host')
    parser.add_argument('--port', dest='port', default='9200', help='elasticsearch port')
    parser.add_argument('--assign_to', dest='nodes', default='127.0.0.1',
                        help='names of nodes to assign index (randomly for multiple nodes)',
                        nargs='+')
    args = parser.parse_args()
    es = ES(args.host, args.port, args.nodes)
    unassigned_shards = es.get_unassigned_indices_and_shards()
    for index, unassigned_shards in unassigned_shards.items():
        for unassigned_shard in unassigned_shards:
            es.allocate_shard(index, unassigned_shard)
