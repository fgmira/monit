from typing import Any, Tuple
from monitoring import models as monit_models
import logging
import requests
import math
from requests.auth import HTTPBasicAuth




class NowInstanceRequests(object):
    def __init__(self, now_instance: monit_models.NowInstance, **kwargs) -> None:
        self.now_instance = now_instance
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username=now_instance.now_inst_user_integration, password=now_instance.now_inst_pwd_integration)
        self.session.headers.update({})
        self.session.verify = False
    def calculate_minutes_ago(self) -> str:
        return f'^sys_updated_onRELATIVEGT@minute@ago@{math.ceil(self.now_instance.now_inst_refresh_time_seconds/60)*3}'
    def get_data(self, context, **kwargs) -> Tuple[requests.Response, str]:
        def join_url():
            if self.now_instance.now_inst_url[-1:] == '/' and context[0] == '/':
                return self.now_instance.now_inst_url[:-1] + context
            return self.now_instance.now_inst_url + context
        url = join_url()
        logging.debug('Start get data from %s', url)
        response = self.session.get(url=url)
        if response.status_code != 200:
            logging.error('Error in get data in instance %s: url: %s, Status Code = "%s", Response Headers = "%s", Response Body = "%s"', self.now_instance.now_inst_name, url, response.status_code, response.headers, response.text)
        logging.debug('Sucess in get data in instance %s: url = "%s", Status Code = "%s", Response Headers = "%s", Response Body = "%s"', self.now_instance.now_inst_name, url, response.status_code, response.headers, response.text)
        logging.info('Sucess in get data in instance %s: url = "%s"', self.now_instance.now_inst_name, url)
        return response, url
    def test_connection(self) -> Tuple[bool, str]:
        response, url = self.get_data(context='/api/now/table/sys_user?sysparm_limit=1')
        if response.status_code !=200:
            return False, f"Unable to connect to instance successfully. See erros: Status Code = {response.status_code}, Response Headers = {response.headers}, Response Body = {response.text}"
        return True, 'Connection test successful'
    def get_instance_nodes(self) -> list[monit_models.NowInstanceNode]:
        context = '/api/now/table/sys_cluster_state?sysparm_display_value=true&sysparm_exclude_reference_link=true'
        response, url = self.get_data(context=context)
        if response.status_code != 200:
            raise Exception (f'Error getting the nodes from the {self.now_instance.now_inst_name} instance: {url=}')
        
        dict_fields = response.json()
        now_nodes_result = []
        for f in dict_fields['result']:
            now_node_db = monit_models.NowInstanceNode(
                ium_active=True,
                now_node_instance=self.now_instance,
                now_node_allow_inbound=True if f['allow_inbound'] == 'true' else False,
                now_node_build_name=f['build_name'],
                now_node_fast_aha_readiness=f['fast_aha_readiness'],
                now_node_id=f['node_id'],
                now_node_type_description=f['node_type'],
                now_node_participation=f['participation'],
                now_node_ready_to_failover=True if f['ready_to_failover'] == 'true' else False,
                now_node_system_id=f['system_id'],
                now_node_now_status=f['status'],
                now_node_schedulers=f['schedulers'],
                now_node_monit_state=monit_models.StateChoices.INIT,
                ium_created_by='system',
                ium_updated_by='system',
            )
            now_nodes_result.append(now_node_db)
        return now_nodes_result

    def get_instance_midservers(self) -> list[monit_models.NowInstanceMidServer]:
        context = '/api/now/table/ecc_agent?sysparm_display_value=true&sysparm_exclude_reference_link=true'
        response, url = self.get_data(context=context)
        if response.status_code != 200:
            raise Exception (f'Error getting the nodes from the {self.now_instance.now_inst_name} instance: {url=}')
        
        dict_fields = response.json()
        now_mid_result = []
        for f in dict_fields['result']:
            now_mid_db = monit_models.NowInstanceMidServer(
                ium_active=True,
                now_mid_instance=self.now_instance,
                now_mid_sys_id=f['sys_id'],
                now_mid_name=f['name'],
                now_mid_now_status=f['status'],
                now_mid_now_validated=f['validated'],
                now_mid_host_name=f['host_name'],
                now_mid_version=f['version'],
                now_mid_ip_address=f['ip_address'],
                now_mid_host_os=f['host_os_distribution'],
                now_mid_started=f['started'],
                now_mid_stopped=f['stopped'],
                now_mid_last_refreshed=f['last_refreshed'],
                now_mid_unresolved_issues=f['unresolved_issues'],
                now_mid_monit_state=monit_models.StateChoices.INIT,
                ium_created_by='system',
                ium_updated_by='system',
            )
            now_mid_result.append(now_mid_db)
        return now_mid_result

    def test_metric_custom_settings(self, metric_custom_setting:monit_models.MetricCustomSetting) -> Tuple[bool, str, dict]:

        def check_fields_type() -> Tuple[bool, str, dict]:
            sysparm_display_value_ck = 'sysparm_display_value=false'
            sysparm_exclude_reference_link_ck = 'sysparm_exclude_reference_link=true'
            sysparm_no_count_ck = f'sysparm_no_count=false'
            sysparm_query_ck = f'sysparm_query=name={metric_custom_setting.mtr_set_cstm_now_table_name}'
            sysparm_fields_ck = f'sysparm_fields=element,internal_type,internal_type.scalar_type'
            context_ck=f'/api/now/table/sys_dictionary?{sysparm_query_ck}&{sysparm_fields_ck}&{sysparm_no_count_ck}&{sysparm_exclude_reference_link_ck}&{sysparm_display_value_ck}'
            response_ck, url_ck = self.get_data(context=context_ck)
            if response_ck.status_code != 200:
                try:
                    response_ck_body = response_ck.json()
                except Exception as e:
                    response_ck_body = f'ERROR IN PARSE RESPONSE: Exception: {e} :: {response.content}'
                result_dic_ck = {'RESPONSE CODE':response_ck.status_code, 'URL': url_ck, 'HEADER': response_ck.headers, 'BODY': response_ck_body}
                return False, 'Error in check fields. See logs', result_dic_ck
            VALID_INTERNAL_TYPES = ['integer','float','decimal','longint',]
            dict_fields = response_ck.json()
            field_ck_list = str(metric_custom_setting.mtr_set_cstm_sysparm_fields).replace(' ','').split(',')
            list_errors = list()
            for ck_f in field_ck_list:
                for f in dict_fields['result']:
                    if f['element'] == ck_f:
                        if not f['internal_type.scalar_type'] in VALID_INTERNAL_TYPES:
                            list_errors.append(f)
            if len(list_errors) != 0:
                return False, f'These fields are of type invalid. Valid types have their scalar_type equal to one of these:{VALID_INTERNAL_TYPES}', {'INVALID FIELD':list_errors}
            else: 
                return True, 'All fields are valid', {}
        
        sysparm_display_value = 'sysparm_display_value=false'
        sysparm_exclude_reference_link = 'sysparm_exclude_reference_link=true'
        sysparm_query = f'sysparm_query={metric_custom_setting.mtr_set_cstm_sysparm_query}{self.calculate_minutes_ago()}'
        sysparm_no_count = f'sysparm_no_count=false'
        if metric_custom_setting.mtr_set_cstm_type == 'COUNT':
            sysparm_field = f'sysparm_fields={metric_custom_setting.mtr_set_cstm_sysparm_fields}'
            sysparm_limit = f'sysparm_limit=1'
            context=f'/api/now/table/{metric_custom_setting.mtr_set_cstm_now_table_name}'
            context=f'{context}?{sysparm_query}&{sysparm_field}&{sysparm_limit}&{sysparm_display_value}&{sysparm_exclude_reference_link}'
        elif metric_custom_setting.mtr_set_cstm_type == 'AGGREG':
            #check fields types
            ck_flag_valid, ck_msg, ck_result = check_fields_type()
            if not(ck_flag_valid):
                return ck_flag_valid, ck_msg, ck_result

            sysparm_avg_fields = f'sysparm_avg_fields={metric_custom_setting.mtr_set_cstm_sysparm_fields}'
            sysparm_min_fields = f'sysparm_min_fields={metric_custom_setting.mtr_set_cstm_sysparm_fields}'
            sysparm_max_fields = f'sysparm_max_fields={metric_custom_setting.mtr_set_cstm_sysparm_fields}'
            sysparm_sum_fields = f'sysparm_sum_fields={metric_custom_setting.mtr_set_cstm_sysparm_fields}'
            sysparm_limit = f'sysparm_limit={metric_custom_setting.mtr_set_cstm_sysparm_limit}'
            context=f'/api/now/stats/{metric_custom_setting.mtr_set_cstm_now_table_name}'
            context=f'{context}?{sysparm_query}&{sysparm_avg_fields}&{sysparm_min_fields}&{sysparm_max_fields}&{sysparm_sum_fields}&{sysparm_limit}&{sysparm_display_value}&{sysparm_exclude_reference_link}'
        response, url = self.get_data(context=context)
        try:
            response_body = response.json()
        except Exception as e:
            response_body = f'ERROR IN PARSE RESPONSE: Exception: {e} :: {response.content}'
        result_dic = {'RESPONSE CODE':response.status_code, 'URL': url, 'HEADER': response.headers, 'BODY': response_body}
        if response.status_code != 200:
            return False, 'Error in test get data to Metric Custom Setting.', result_dic
        try:
            if response.json() == {'result': {}}:
                return False, 'Error in test get data to Metric Custom Setting. No data returned in this query', result_dic
        except:
                return False, 'Error in test get data to Metric Custom Setting. Error in parse response', result_dic
        return True, 'Sucess in test get data to Metric Custom Setting', result_dic
