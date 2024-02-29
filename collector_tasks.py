from __future__ import absolute_import, unicode_literals

import logging
from multiprocessing import context
from typing import Any
from requests import Response
from celery import shared_task
from monitoring.monitoring_modules import service_now

import monitoring.models as monit_models
import xmltodict
import dateparser
from pytz import timezone
from datetime import datetime
import json


@shared_task()
def refresh_mids(now_instance_ium_uuid: str) -> None:
    logging.info(f'Start refresh_mids - {now_instance_ium_uuid=}')
    now_instance = monit_models.NowInstance.objects.get(ium_uuid=now_instance_ium_uuid)
    if now_instance == None:
        raise Exception(f'No instance to {now_instance_ium_uuid}')
    now_instance_r = service_now.NowInstanceRequests(now_instance=now_instance)
    try:
        mids_list_instance = now_instance_r.get_instance_midservers()
    except Exception as e:
        monit_evt = monit_models.MonitoringEvents(
            monit_evt_now_instance=now_instance,
            monit_evt_source_type=monit_models.SourceTypeChoices.INSTANCE,
            monit_evt_source_ium_uuid=now_instance_ium_uuid,
            monit_evt_meta_data_1='ACCESS ERROR',
            monit_evt_meta_data_2='REFRESH MIDS',
            monit_evt_value=1,
            monit_evt_stats_type='count',
            monit_evt_unit='ACCESS',
            ium_updated_by = 'system',
            ium_created_by = 'system',
        )
        monit_evt.save()
        now_instance.now_inst_monit_state = monit_models.StateChoices.ERRR
        now_instance.ium_updated_by = 'system'
        now_instance.save()
        raise e
    list_mid_instance_id = list()
    for mid_instance in mids_list_instance:
        mid_db = monit_models.NowInstanceMidServer.objects.get(now_mid_sys_id=mid_instance.now_mid_sys_id)
        if mid_db == None:
            monit_evt = monit_models.MonitoringEvents(
                monit_evt_now_instance=now_instance,
                monit_evt_source_type=monit_models.SourceTypeChoices.INSTANCE,
                monit_evt_source_ium_uuid=None,
                monit_evt_meta_data_1='NEW MID SERVER',
                monit_evt_meta_data_2=f'{mid_instance.now_mid_name}',
                monit_evt_value=1,
                monit_evt_stats_type='count',
                monit_evt_unit='MID',
                ium_updated_by = 'system',
                ium_created_by = 'system',
            )
            monit_evt.save()
            mid_instance.save()
            logging.info(f'New MID Server to {now_instance}: {mid_instance.now_mid_name}')
        elif str(mid_instance.now_mid_now_status).lower() != 'up':
            logging.warning(f'Now MID Server {mid_instance.now_mid_name} is not online. {mid_instance.now_mid_now_status}')
            monit_evt = monit_models.MonitoringEvents(
                monit_evt_now_instance=now_instance,
                monit_evt_source_type=monit_models.SourceTypeChoices.MID,
                monit_evt_source_ium_uuid=mid_db.ium_uuid,
                monit_evt_meta_data_1='MID SERVER NOT ONLINE',
                monit_evt_meta_data_2=f'{mid_instance.now_mid_name}',
                monit_evt_meta_data_3=f'{mid_instance.now_mid_now_status=}'.replace("'",""),
                monit_evt_value=1,
                monit_evt_stats_type='count',
                monit_evt_unit='MID',
                ium_updated_by = 'system',
                ium_created_by = 'system',
            )
            monit_evt.save()
            mid_db.now_mid_now_status = mid_instance.now_mid_now_status
            mid_db.now_mid_monit_state = monit_models.StateChoices.WARN
            mid_db.ium_updated_by = 'system'
            mid_db.save()
        elif str(mid_instance.now_mid_now_status).lower() == 'up' and mid_db.now_mid_now_status != mid_instance.now_mid_now_status:
            mid_db.now_mid_now_status = mid_instance.now_mid_now_status
            mid_db.now_mid_monit_state = monit_models.StateChoices.OPER
            mid_db.ium_updated_by = 'system'
            mid_db.save()
        elif str(mid_instance.now_mid_now_status) != monit_models.StateChoices.OPER and str(mid_instance.now_mid_now_status).lower() == 'up':
            mid_db.now_mid_now_status = mid_instance.now_mid_now_status
            mid_db.now_mid_monit_state = monit_models.StateChoices.OPER
            mid_db.ium_updated_by = 'system'
            mid_db.save()
        list_mid_instance_id.append(mid_instance.now_mid_sys_id)
    list_mids_db = monit_models.NowInstanceMidServer.objects.filter(now_mid_instance=now_instance_ium_uuid)
    for mid_db in list_mids_db:
        if mid_db.now_mid_sys_id not in list_mid_instance_id:
            logging.warning(f'Now Node {mid_db.now_mid_sys_id} not in the instance. {now_instance}')
            monit_evt = monit_models.MonitoringEvents(
                monit_evt_now_instance=now_instance,
                monit_evt_source_type=monit_models.SourceTypeChoices.MID,
                monit_evt_source_ium_uuid=mid_db.ium_uuid,
                monit_evt_meta_data_1='MIDSERVER NOT IN INSTANCE',
                monit_evt_meta_data_2=f'{mid_db.now_mid_sys_id}',
                monit_evt_value=1,
                monit_evt_unit='MID',
                monit_evt_stats_type='count',
                ium_updated_by = 'system',
                ium_created_by = 'system',
            )
            monit_evt.save()
            mid_db.now_mid_now_status = 'None'
            mid_db.now_mid_now_validated = 'None'
            mid_db.now_mid_monit_state = monit_models.StateChoices.ERRR
            mid_db.ium_updated_by = 'system'
            mid_db.save()
    
    now_instance.now_inst_monit_state = monit_models.StateChoices.OPER
    now_instance.ium_updated_by = 'system'
    now_instance.save()




    logging.info(f'Finish refresh_mids - {now_instance_ium_uuid=}')


@shared_task()
def refresh_nodes(now_instance_ium_uuid: str) -> None:
    logging.info(f'Start refresh_nodes - {now_instance_ium_uuid=}')
    now_instance = monit_models.NowInstance.objects.get(ium_uuid=now_instance_ium_uuid)
    if now_instance == None:
        raise Exception(f'No instance to {now_instance_ium_uuid}')
    now_instance_r = service_now.NowInstanceRequests(now_instance=now_instance)
    try:
        nodes_list_instance = now_instance_r.get_instance_nodes()
    except Exception as e:
        monit_evt = monit_models.MonitoringEvents(
            monit_evt_now_instance=now_instance,
            monit_evt_source_type=monit_models.SourceTypeChoices.INSTANCE,
            monit_evt_source_ium_uuid=now_instance_ium_uuid,
            monit_evt_meta_data_1='ACCESS ERROR',
            monit_evt_meta_data_2='REFRESH NODES',
            monit_evt_stats_type='count',
            monit_evt_value=1,
            monit_evt_unit='ACCESS',
            ium_updated_by = 'system',
            ium_created_by = 'system',
        )
        monit_evt.save()
        now_instance.now_inst_monit_state = monit_models.StateChoices.ERRR
        now_instance.ium_updated_by = 'system'
        now_instance.save()
        raise e

    list_node_instance_id = list()
    for node_instance in nodes_list_instance:
        node_db = monit_models.NowInstanceNode.objects.get(now_node_id=node_instance.now_node_id)
        if node_db == None:
            monit_evt = monit_models.MonitoringEvents(
                monit_evt_now_instance=now_instance,
                monit_evt_source_type='INSTANCE',
                monit_evt_source_ium_uuid=None,
                monit_evt_meta_data_1='NEW NODE',
                monit_evt_meta_data_2=f'{node_instance.now_node_system_id}',
                monit_evt_value=1,
                monit_evt_unit='NODE',
                ium_updated_by = 'system',
                ium_created_by = 'system',
            )
            monit_evt.save()
            node_instance.save()
            logging.info(f'New Node to {now_instance}: {node_instance.now_node_system_id}')
        elif str(node_instance.now_node_now_status).lower() != 'online':
            logging.warning(f'Now Node {node_instance.now_node_system_id} is not online. {node_instance.now_node_now_status}')
            monit_evt = monit_models.MonitoringEvents(
                monit_evt_now_instance=now_instance,
                monit_evt_source_type=monit_models.SourceTypeChoices.NODE,
                monit_evt_source_ium_uuid=node_db.ium_uuid,
                monit_evt_meta_data_1='NODE NOT ONLINE',
                monit_evt_meta_data_2=f'{node_instance.now_node_system_id}',
                monit_evt_meta_data_3=f'{node_instance.now_node_now_status=}'.replace("'",""),
                monit_evt_stats_type='count',
                monit_evt_value=1,
                monit_evt_unit='NODE',
                ium_updated_by = 'system',
                ium_created_by = 'system',
            )
            monit_evt.save()
            node_db.now_node_now_status = node_instance.now_node_now_status
            node_db.now_node_monit_state = monit_models.StateChoices.WARN
            node_db.ium_updated_by = 'system'
            node_db.save()
        elif str(node_instance.now_node_now_status).lower() == 'online' and node_db.now_node_now_status != node_instance.now_node_now_status:
            node_db.now_node_now_status = node_instance.now_node_now_status
            node_db.now_node_monit_state = monit_models.StateChoices.OPER
            node_db.ium_updated_by = 'system'
            node_db.save()
        elif str(node_instance.now_node_monit_state) != monit_models.StateChoices.OPER and str(node_instance.now_node_now_status).lower() == 'online':
            node_db.now_node_now_status = node_instance.now_node_now_status
            node_db.now_node_monit_state = monit_models.StateChoices.OPER
            node_db.ium_updated_by = 'system'
            node_db.save()
        list_node_instance_id.append(node_instance.now_node_id)
    list_nodes_db = monit_models.NowInstanceNode.objects.filter(now_node_instance=now_instance_ium_uuid)
    for node_db in list_nodes_db:
        if node_db.now_node_id not in list_node_instance_id:
            logging.warning(f'Now Node {node_db.now_node_system_id} not in the instance. {now_instance}')
            monit_evt = monit_models.MonitoringEvents(
                monit_evt_now_instance=now_instance,
                monit_evt_source_type=monit_models.SourceTypeChoices.NODE,
                monit_evt_source_ium_uuid=node_db.ium_uuid,
                monit_evt_meta_data_1='NODE NOT IN INSTANCE',
                monit_evt_meta_data_2=f'{node_db.now_node_system_id}',
                monit_evt_stats_type='count',
                monit_evt_value=1,
                monit_evt_unit='NODE',
                ium_updated_by = 'system',
                ium_created_by = 'system',
            )
            monit_evt.save()
            node_db.now_node_now_status = 'None'
            node_db.now_node_monit_state = monit_models.StateChoices.ERRR
            node_db.ium_updated_by = 'system'
            node_db.save()
    
    now_instance.now_inst_monit_state = monit_models.StateChoices.OPER
    now_instance.ium_updated_by = 'system'
    now_instance.save()


    logging.info(f'Finish refresh_nodes - {now_instance_ium_uuid=}')

@shared_task()
def get_node_statistic(node_ium_uuid: str):
    def insert_event(meta_1:str, meta_2:str, meta_3:str, value:float, unit:str, stats_type: str):
        monit_evt = monit_models.MonitoringEvents(
            monit_evt_now_instance=now_instance,
            monit_evt_source_type=monit_models.SourceTypeChoices.NODE,
            monit_evt_source_ium_uuid=node_ium_uuid,
            monit_evt_meta_data_1=meta_1,
            monit_evt_meta_data_2=meta_2,
            monit_evt_meta_data_3=meta_3,
            monit_evt_stats_type=stats_type,
            monit_evt_value=value,
            monit_evt_unit=unit,
            ium_updated_by = 'system',
            ium_created_by = 'system',
        )
        monit_evt.save()
    logging.info(f'Start get_node_statistic to {node_ium_uuid=}')
    now_instance_node = monit_models.NowInstanceNode.objects.get(ium_uuid = node_ium_uuid)
    now_instance = monit_models.NowInstance.objects.get(ium_uuid=now_instance_node.now_node_instance.ium_uuid)
    now_instance_r = service_now.NowInstanceRequests(now_instance=now_instance)
    context = f'/api/now/table/sys_cluster_node_stats?sysparm_query=node_id={now_instance_node.now_node_id}&sysparm_fields=stats&sysparm_display_value=False&sysparm_exclude_reference_link=True&sysparm_no_count=True'
    response, msg = now_instance_r.get_data(context=context)
    if response.status_code != 200:
        insert_event(meta_1='ACCESS ERROR', meta_2='NODE STATS', meta_3='', value=float(1), unit='ACCESS')
        now_instance.now_inst_monit_state = monit_models.StateChoices.ERRR
        now_instance.ium_updated_by = 'system'
        now_instance.save()
        now_instance_node.now_node_monit_state = monit_models.StateChoices.WARN
        now_instance_node.ium_updated_by = 'system'
        now_instance_node.save()
        raise Exception(f'Error getting the stats from the {node_ium_uuid=} node: Error in get data from now instance: {msg=}')
    dict_body = response.json()
    logging.debug(f'{dict_body=}')
    dict_stats = xmltodict.parse(dict_body['result'][0]['stats'])
    dict_stats = dict_stats['xmlstats']
    logging.debug(f'{dict_stats=}')
    try:
        #time delta entre o momento atual no PDT e a data vinda do XML de stats do service now
        logging.debug(f'Insert MonitoringEvents:stats.created.timedelta')
        insert_event(meta_1='stats.created.timedelta', meta_2='', meta_3='', 
            value=float(
                (datetime.now().astimezone(timezone('US/Pacific')) - \
                    dateparser.parse(dict_stats['@created'])).seconds
                    ), 
            unit='SECONDS',stats_type='count')

        #estatisticas de fila de processamento
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:scheduler.queue.age')
        insert_event(meta_1='scheduler.queue.age', meta_2='', meta_3='', value=float(dict_stats['scheduler.queue.age']['@count']), unit='TIME',stats_type='count')
        insert_event(meta_1='scheduler.queue.age', meta_2='', meta_3='', value=float(dict_stats['scheduler.queue.age']['@max']), unit='TIME',stats_type='max')
        insert_event(meta_1='scheduler.queue.age', meta_2='', meta_3='', value=float(dict_stats['scheduler.queue.age']['@mean']), unit='TIME',stats_type='mean')
        insert_event(meta_1='scheduler.queue.age', meta_2='', meta_3='', value=float(dict_stats['scheduler.queue.age']['@median']), unit='TIME',stats_type='median')
        insert_event(meta_1='scheduler.queue.age', meta_2='', meta_3='', value=float(dict_stats['scheduler.queue.age']['@min']), unit='TIME',stats_type='min')

        #estatisticas de fila atrasada de processamento
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:scheduler.queue.overdue_age')
        insert_event(meta_1='scheduler.queue.overdue_age', meta_2='', meta_3='', value=float(dict_stats['scheduler.queue.overdue_age']['@count']), unit='TIME',stats_type='count')
        insert_event(meta_1='scheduler.queue.overdue_age', meta_2='', meta_3='', value=float(dict_stats['scheduler.queue.overdue_age']['@max']), unit='TIME',stats_type='max')
        insert_event(meta_1='scheduler.queue.overdue_age', meta_2='', meta_3='', value=float(dict_stats['scheduler.queue.overdue_age']['@mean']), unit='TIME',stats_type='mean')
        insert_event(meta_1='scheduler.queue.overdue_age', meta_2='', meta_3='', value=float(dict_stats['scheduler.queue.overdue_age']['@median']), unit='TIME',stats_type='median')
        insert_event(meta_1='scheduler.queue.overdue_age', meta_2='', meta_3='', value=float(dict_stats['scheduler.queue.overdue_age']['@min']), unit='TIME',stats_type='min')

        #tamanho da fila
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:scheduler.queue.length')
        insert_event(meta_1='scheduler.queue.length', meta_2='', meta_3='', value=float(dict_stats['scheduler.queue.length']), unit='JOBS',stats_type='count')
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:scheduler.mean.queue.age')
        insert_event(meta_1='scheduler.mean.queue.age', meta_2='', meta_3='', value=float(dict_stats['scheduler.mean.queue.age']), unit='TIME',stats_type='count')

        #jobs processados
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:scheduler.total')
        insert_event(meta_1='scheduler.total.jobs', meta_2='', meta_3='', value=float(dict_stats['scheduler.total.jobs']), unit='JOBS',stats_type='count')
        insert_event(meta_1='scheduler.total.claimed_jobs', meta_2='', meta_3='scheduler.total.claimed_jobs', value=float(dict_stats['scheduler.total.claimed_jobs']), unit='JOBS',stats_type='count')
        insert_event(meta_1='scheduler.total.released_jobs', meta_2='', meta_3='', value=float(dict_stats['scheduler.total.released_jobs']), unit='JOBS',stats_type='count')
        insert_event(meta_1='scheduler.total.burst.workers', meta_2='', meta_3='', value=float(dict_stats['scheduler.total.burst.workers']), unit='JOBS',stats_type='count')

        #schedulers
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:scheduler.workers')
        for schedule_key in dict_stats['scheduler.workers'].keys():
            insert_event(meta_1='scheduler.workers', meta_2=schedule_key, meta_3='total.jobs', value=float(dict_stats['scheduler.workers'][schedule_key]['total.jobs']), unit='JOBS',stats_type='count')
            insert_event(meta_1='scheduler.workers', meta_2=schedule_key, meta_3='mean.duration', value=float(dict_stats['scheduler.workers'][schedule_key]['mean.duration']), unit='MILISECONDS',stats_type='count')

        #db
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:db_poll')
        for db_poll in dict_stats['db.pools']['pool']:
            insert_event(meta_1='db_poll', meta_2=db_poll['@name'], meta_3='available', value=float(db_poll['available']), unit='CONNECTIONS',stats_type='count')
            insert_event(meta_1='db_poll', meta_2=db_poll['@name'], meta_3='busy', value=float(db_poll['busy']), unit='CONNECTIONS',stats_type='count')
            insert_event(meta_1='db_poll', meta_2=db_poll['@name'], meta_3='max', value=float(db_poll['max']), unit='CONNECTIONS',stats_type='count')
        
        #system memory
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:system.memory')
        insert_event(meta_1='system.memory', meta_2='', meta_3='', value=float(dict_stats['system.memory.max'][0]), unit='MB',stats_type='max')
        insert_event(meta_1='system.memory', meta_2='', meta_3='', value=float(dict_stats['system.memory.total']), unit='MB',stats_type='total')
        insert_event(meta_1='system.memory', meta_2='', meta_3='', value=float(dict_stats['system.memory.in.use']), unit='MB',stats_type='in use')
        insert_event(meta_1='system.memory', meta_2='', meta_3='', value=float(dict_stats['system.memory.pct.free']), unit='MB',stats_type='pct free')

        #system memory meta_space
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:system.memory_metaspace')
        insert_event(meta_1='system.memory_metaspace', meta_2='', meta_3='', value=float(dict_stats['system.memory_metaspace.max']), unit='MB',stats_type='max')
        insert_event(meta_1='system.memory_metaspace', meta_2='', meta_3='', value=float(dict_stats['system.memory_metaspace.total']), unit='MB',stats_type='total')
        insert_event(meta_1='system.memory_metaspace', meta_2='', meta_3='', value=float(dict_stats['system.memory_metaspace.in.use']), unit='MB',stats_type='in use')


        #diagnostic events
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:diagnostic.events')
        num_event_info = 0
        num_event_warn = 0
        num_event_err = 0
        for event in dict_stats['diagnostic.events']['event']:
            if event['severity'] == 'Information':
                num_event_info += 1
            if event['severity'] == 'Warning':
                num_event_warn += 1
            if event['severity'] == 'Error':
                num_event_err += 1
        insert_event(meta_1='diagnostic.events', meta_2='Information', meta_3='', value=float(num_event_info), unit='EVENTS',stats_type='count')
        insert_event(meta_1='diagnostic.events', meta_2='Warning', meta_3='', value=float(num_event_warn), unit='EVENTS',stats_type='count')
        insert_event(meta_1='diagnostic.events', meta_2='Error', meta_3='', value=float(num_event_err), unit='EVENTS',stats_type='count')

        #servlet
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:servlet.*')
        insert_event(meta_1='servlet.transactions', meta_2='', meta_3='', value=float(dict_stats['servlet.transactions']), unit='TRANSACTIONS',stats_type='count')
        insert_event(meta_1='servlet.errors.handled', meta_2='', meta_3='', value=float(dict_stats['servlet.errors.handled']), unit='ERRORS',stats_type='count')
        insert_event(meta_1='servlet.processor.transactions', meta_2='', meta_3='', value=float(dict_stats['servlet.processor.transactions']), unit='TRANSACTIONS',stats_type='count')
        insert_event(meta_1='servlet.cancelled.transactions', meta_2='', meta_3='', value=float(dict_stats['servlet.cancelled.transactions']), unit='TRANSACTIONS',stats_type='count')
        insert_event(meta_1='servlet.active.sessions', meta_2='', meta_3='', value=float(dict_stats['servlet.active.sessions']), unit='SESSIONS',stats_type='count')

        #servlet metrics
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:servlet.metrics.*')
        servlet_metrics = dict_stats['servlet.metrics']
        for key_metric in servlet_metrics.keys():
            if type(servlet_metrics[key_metric]) == dict:
                for stats_time_key in servlet_metrics[key_metric].keys():
                    for stats_key in servlet_metrics[key_metric][stats_time_key].keys():
                        try:
                            insert_event(meta_1=f'servlet.metrics.{key_metric}', meta_2=stats_time_key, meta_3='', value=float(servlet_metrics[key_metric][stats_time_key][stats_key]), unit='GENERIC',stats_type=stats_key)
                        except ValueError:
                            logging.warning(f'Insert MonitoringEvents to {node_ium_uuid=}: Could not convert string to float: {servlet_metrics[key_metric][stats_time_key][stats_key]} - meta_1=servlet.metrics.{key_metric}, meta_2={stats_time_key}, meta_3={stats_key}')
            elif type(servlet_metrics[key_metric]) == list:
                for i in range(len(servlet_metrics[key_metric])):
                    for stats_time_key in servlet_metrics[key_metric][i]:
                        for stats_key in servlet_metrics[key_metric][i][stats_time_key].keys():
                            try:
                                insert_event(meta_1=f'servlet.metrics.{key_metric}_{i}', meta_2=stats_time_key, meta_3='', value=float(servlet_metrics[key_metric][i][stats_time_key][stats_key]), unit='GENERIC',stats_type=stats_key)
                            except ValueError:
                                logging.warning(f'Insert MonitoringEvents to {node_ium_uuid=}: Could not convert string to float: {servlet_metrics[key_metric][stats_time_key][stats_key]} - meta_1=servlet.metrics.{key_metric}, meta_2={stats_time_key}, meta_3={stats_key}')
            else:
                raise Exception(f'Type of {servlet_metrics[key_metric]=} unforeseen: {type(servlet_metrics[key_metric])}')

        #semaphores
        logging.debug(f'Insert MonitoringEvents to {node_ium_uuid=}:semaphore.*')
        semaphores_list = dict_stats['semaphores']
        for semaphore in semaphores_list:
            insert_event(meta_1=f'semaphore.{semaphore["@name"]}', meta_2='available', meta_3='', value=float(semaphore['@available']), unit='SEMAPHORES',stats_type='count')
            insert_event(meta_1=f'semaphore.{semaphore["@name"]}', meta_2='queue_depth', meta_3='', value=float(semaphore['@queue_depth']), unit='TRANSACTIONS',stats_type='count')
            insert_event(meta_1=f'semaphore.{semaphore["@name"]}', meta_2='queue_age', meta_3='', value=float(semaphore['@queue_age']), unit='MILISECONDS',stats_type='count')
            insert_event(meta_1=f'semaphore.{semaphore["@name"]}', meta_2='rejected_executions', meta_3='', value=float(semaphore['@rejected_executions']), unit='EXECUTIONS',stats_type='count')
    except Exception as e:
        insert_event(meta_1='CREATE STATS', meta_2='NODE STATS', meta_3='', value=float(1), unit='ACCESS',stats_type='count')
        now_instance_node.ium_updated_by = 'system'
        now_instance_node.now_node_monit_state = monit_models.StateChoices.WARN
        now_instance_node.save()
        raise e
    #insert_event(meta_1='', meta_2='', meta_3='', value=float(dict_stats['']), unit='')
    logging.info(f'Finish get_node_statistic to {node_ium_uuid=}')

@shared_task
def get_metric_custom(metric_custom_ium_uuid: str):
    def insert_event(meta_1:str, meta_2:str, meta_3:str, value:float, unit:str, stats_type:str):
        monit_evt = monit_models.MonitoringEvents(
            monit_evt_now_instance=now_instance,
            monit_evt_source_type=monit_models.SourceTypeChoices.MTRCSTM,
            monit_evt_source_ium_uuid=metric_custom_ium_uuid,
            monit_evt_meta_data_1=meta_1,
            monit_evt_meta_data_2=meta_2,
            monit_evt_meta_data_3=meta_3,
            monit_evt_stats_type=stats_type,
            monit_evt_value=value,
            monit_evt_unit=unit,
            ium_updated_by = 'system',
            ium_created_by = 'system',
        )
        monit_evt.save()
    logging.info(f'Start get_metric_custom to {metric_custom_ium_uuid=}')
    metric_custom_setting = monit_models.MetricCustomSetting.objects.get(ium_uuid=metric_custom_ium_uuid)
    now_instance = monit_models.NowInstance.objects.get(ium_uuid=metric_custom_setting.mtr_set_cstm_now_inst.ium_uuid)
    now_instance_r = service_now.NowInstanceRequests(now_instance=now_instance)

    sysparm_display_value = 'sysparm_display_value=false'
    sysparm_exclude_reference_link = 'sysparm_exclude_reference_link=true'
    sysparm_query = f'sysparm_query={metric_custom_setting.mtr_set_cstm_sysparm_query}{now_instance_r.calculate_minutes_ago()}'

    if metric_custom_setting.mtr_set_cstm_type == 'COUNT':
        sysparm_field = f'sysparm_fields={metric_custom_setting.mtr_set_cstm_sysparm_fields}'
        sysparm_limit = f'sysparm_limit=1'
        context=f'/api/now/table/{metric_custom_setting.mtr_set_cstm_now_table_name}'
        context=f'{context}?{sysparm_query}&{sysparm_field}&{sysparm_limit}&{sysparm_display_value}&{sysparm_exclude_reference_link}'
    elif metric_custom_setting.mtr_set_cstm_type == 'AGGREG':
        sysparm_no_count = f'sysparm_no_count=false'
        sysparm_avg_fields = f'sysparm_avg_fields={metric_custom_setting.mtr_set_cstm_sysparm_fields}'
        sysparm_min_fields = f'sysparm_min_fields={metric_custom_setting.mtr_set_cstm_sysparm_fields}'
        sysparm_max_fields = f'sysparm_max_fields={metric_custom_setting.mtr_set_cstm_sysparm_fields}'
        sysparm_sum_fields = f'sysparm_sum_fields={metric_custom_setting.mtr_set_cstm_sysparm_fields}'
        sysparm_limit = f'sysparm_limit={metric_custom_setting.mtr_set_cstm_sysparm_limit}'
        context=f'/api/now/stats/{metric_custom_setting.mtr_set_cstm_now_table_name}'
        context=f'{context}?{sysparm_query}&{sysparm_avg_fields}&{sysparm_min_fields}&{sysparm_max_fields}&{sysparm_sum_fields}&{sysparm_limit}&{sysparm_display_value}&{sysparm_exclude_reference_link}&{sysparm_no_count}'
    else:
        raise Exception(f'Error getting the metric custom from the {metric_custom_ium_uuid=} metric custom: For the type of metric informed there is no expected processing: {metric_custom_setting.mtr_set_cstm_type=}')
    
    response, msg = now_instance_r.get_data(context=context)
    if response.status_code != 200:
        insert_event(meta_1='ACCESS ERROR', meta_2='NODE STATS', meta_3='', value=float(1), unit='ACCESS',stats_type='count')
        now_instance.now_inst_monit_state = monit_models.StateChoices.ERRR
        now_instance.ium_updated_by = 'system'
        now_instance.save()
        raise Exception(f'Error getting the metric custom from the {metric_custom_ium_uuid=} metric custom: Error to get data in now instance: {msg=}')
    try:
        if metric_custom_setting.mtr_set_cstm_type == 'COUNT':
            insert_event(meta_1=metric_custom_setting.mtr_set_cstm_name, meta_2=metric_custom_setting.mtr_set_cstm_now_table_name, meta_3='', value=float(response.headers['X-Total-Count']), unit='ROWS',stats_type='count')
        elif metric_custom_setting.mtr_set_cstm_type == 'AGGREG':
            dict_response = response.json()
            dict_stats = dict_response['result']['stats']
            for stats_key in dict_stats.keys():
                if stats_key == 'count':
                    insert_event(meta_1=metric_custom_setting.mtr_set_cstm_name, meta_2=metric_custom_setting.mtr_set_cstm_now_table_name, meta_3='', value=float(dict_stats[stats_key]), unit='ROWS',stats_type='count')
                else:
                    for field in dict_stats[stats_key].keys():
                        try:
                            insert_event(meta_1=metric_custom_setting.mtr_set_cstm_name, meta_2=f'{metric_custom_setting.mtr_set_cstm_now_table_name}.{field}', meta_3='', value=float(dict_stats[stats_key][field]), unit='GENERIC',stats_type=stats_key)
                        except ValueError:
                            logging.warning(f'Insert MonitoringEvents to {metric_custom_ium_uuid=}: Could not convert string to float: {dict_stats[stats_key][field]} - meta_1={metric_custom_setting.mtr_set_cstm_name}, meta_2={metric_custom_setting.mtr_set_cstm_now_table_name}.{field}, meta_3={stats_key}')

        else:
            raise Exception(f'Error getting the metric custom from the {metric_custom_ium_uuid=} metric custom: For the type of metric informed there is no expected processing: {metric_custom_setting.mtr_set_cstm_type=}')
    except Exception as e:
        insert_event(meta_1='CREATE METRIC CUSTOM', meta_2=metric_custom_setting.mtr_set_cstm_name, meta_3='', value=float(1), unit='ACCESS',stats_type='count')
        now_instance.ium_updated_by = 'system'
        now_instance.now_node_monit_state = monit_models.StateChoices.WARN
        now_instance.save()
        raise e
