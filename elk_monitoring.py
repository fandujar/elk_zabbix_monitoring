#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyzabbix import ZabbixMetric, ZabbixSender
from elasticsearch import Elasticsearch

import socket
import json
import sched, time
import logging

#NODE_DATA = ['ram.percent','heap.percent','load_1m','cpu']
#ALLOCATION_DATA = ['disk.avail','disk.used','disk.percent','disk.indices']

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class ElkMonitoring():
    def __init__(self,host=socket.gethostname().split(".")[0],port='9200'):
        self.hostname = host
        cluster = ['{0}:{1}'.format(host,port)]
        self.es = Elasticsearch(cluster)

    def cat_node(self):
        try:
            nodes = self.es.cat.nodes(v='v',format='json')
            node_stats = [n for n in nodes if n['name'] == self.hostname]
            node_stats = replace_key('es.cat.nodes', node_stats)
            metrics = zabbix_metrics(self.hostname,node_stats)

            return node_stats, metrics
        except Exception as e:
            log.error(e)

    def cat_master(self):
        try:
            master= self.es.cat.master(v='v',format='json')
            master = replace_key('es.cat.master', master)
            metrics = zabbix_metrics(self.hostname,master)

            return master, metrics

        except Exception as e:
            log.error(e)
        

    def cat_allocation(self):
        try:
            allocation = self.es.cat.allocation(v='v',format='json',node_id='{}'.format(self.hostname))
            allocation = replace_key('es.cat.allocation', allocation)
            metrics = zabbix_metrics(self.hostname, allocation)

            return allocation, metrics
        except Exception as e:
            log.error(e)

    def cat_health(self):
        try:
            cluster_health = self.es.cat.health(v='v',format='json')
            cluster_health = replace_key('es.cat.health',cluster_health)
            metrics = zabbix_metrics(self.hostname,cluster_health)
            return cluster_health, metrics
        except Exception as e:
            log.error(e)


def replace_key(namespace, list):
    try:    
        for item in list:
            for key, values in item.items():
                old_key, new_key = key, '{}.{}'.format(namespace,key)
                item[new_key] = item[old_key]
                log.debug('replaced key {} by {}'.format(old_key,new_key))
                del item[old_key]
        
        return list
    except Exception as e:
        log.error(e)

def zabbix_metrics(hostname, list):
    metrics = []
    try:
        for item in list:
            for key, value in item.items():
                m = ZabbixMetric(hostname,key, value)
                log.debug('new zabbix metric {}'.format(m))
                metrics.append(m)

        return metrics
    except Exception as e:
        log.error(e)

def send_to_zabbix(metrics):
    zabbix = ZabbixSender(use_config=True)
    try:
        if len(metrics) >= 1:
            log.debug('about to send {} to zabbix'.format(metrics))
            zabbix.send(metrics)
            log.info('sent metrics to zabbix')
        else:
            log.warning('metrics size invalid')
    except Exception as e:
        log.error(e)

def main():
    em = ElkMonitoring()
    s = sched.scheduler(time.time, time.sleep)
    while True:
        try:
            _, cat_allocation_metrics = em.cat_allocation()
            _, cat_health_metrics = em.cat_health()
            _, cat_node_metrics = em.cat_node()
            _, cat_master_metrics = em.cat_master()
            send_to_zabbix(cat_allocation_metrics)
            send_to_zabbix(cat_health_metrics)
            send_to_zabbix(cat_node_metrics)
            send_to_zabbix(cat_master_metrics)
            time.sleep(30)
        except Exception as e:
            log.error(e)

if __name__ == "__main__":
    main()
