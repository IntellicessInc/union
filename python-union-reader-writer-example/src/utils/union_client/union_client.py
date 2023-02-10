import json
import logging
from datetime import datetime

import requests
from requests import Response, ReadTimeout
from requests.exceptions import ChunkedEncodingError

from src.utils import jwlf
from src.utils.union_client.saved_jwlf_log import SavedJWLFLog


class UnionClient(object):
    __MAX_ACCESS_TOKEN_AGE_SECONDS__ = 60
    __MAX_NUMBER_OF_IDS_PER_REQUEST__ = 25

    def __init__(self, union_url, auth_provider_url, auth_provider_realm, auth_client_id, username, password):
        self.union_url = union_url
        self.auth_provider_url = auth_provider_url
        self.auth_provider_realm = auth_provider_realm
        self.auth_client_id = auth_client_id
        self.username = username
        self.password = password
        self.last_access_token = None
        self.access_token_creation_time = None
        self.search_since_timestamp_dict = {}

    def save_jwlf_log(self, client, region, asset, folder, jwlf_log: jwlf.JWLFLog):
        jwlf_log_json = jwlf_log.to_dict()
        url = f"{self.union_url}/api/v1/well-logs/{client}/{region}/{asset}/{folder}"
        access_token = self.__get_access_token__()
        logging.info(f"{datetime.now()} Saving new data with name={jwlf_log.header.name}")
        res: Response = requests.post(url, json=jwlf_log_json, headers={"Authorization": f"Bearer {access_token}"})
        if res.status_code >= 300:
            raise Exception(
                f"UnionClient#save_jwlf_log failure with http response status code={res.status_code} and response={res.text}")
        return res.json()["id"]

    def save_jwlf_logs(self, client, region, asset, folder, jwlf_logs: [jwlf.JWLFLog]):
        jwlf_log_dict = [jwlf_log.to_dict() for jwlf_log in jwlf_logs]
        url = f"{self.union_url}/api/v1/well-logs/{client}/{region}/{asset}/{folder}/batch-save"
        access_token = self.__get_access_token__()
        logging.info(f"{datetime.now()} Saving new data with names={[jwlf_log.header.name for jwlf_log in jwlf_logs]}")
        res: Response = requests.post(url, json=jwlf_log_dict, headers={"Authorization": f"Bearer {access_token}"})
        if res.status_code >= 300:
            raise Exception(
                f"UnionClient#save_jwlf_logs failure with http response status code={res.status_code} and response={res.text}")
        return [saved_log['id'] for saved_log in res.json()["list"]]

    def get_jwlfs_stream(self, client, region, asset, folder, inclusive_since_timestamp):
        resume_token = None
        query_params = {'fullData': 'true'}
        if inclusive_since_timestamp is not None:
            query_params['resumeAtTimestamp'] = inclusive_since_timestamp
        url = f"{self.union_url}/api/v1/well-logs-stream/{client}/{region}/{asset}/{folder}"
        while True:
            access_token = self.__get_access_token__()
            headers = {'Accept': 'application/x-ndjson', 'Authorization': f"Bearer {access_token}",
                       "Connection": "close"}
            if resume_token is not None:
                query_params['resumeToken'] = resume_token
            logging.debug(f"{datetime.now()} Stream created")
            try:
                with requests.get(url, stream=True, params=query_params, headers=headers, timeout=90) as response:
                    for line in response.iter_lines(decode_unicode=True):
                        if line and line != '':
                            new_event = json.loads(line)
                            resume_token = new_event['id']
                            log: SavedJWLFLog = SavedJWLFLog.from_dict(new_event['data'])
                            yield log
            except ChunkedEncodingError:
                logging.debug("Stream time limit got exceeded in the shape of ChunkedEncodingError")
                pass
            except ReadTimeout:
                logging.debug("Stream time limit got exceeded in the shape of ReadTimeout")
                pass
            finally:
                logging.debug("Stream will be recreated as its time limit got exceeded")

    def get_new_jwlf_logs_with_data(self, client, region, asset, folder, inclusive_since_timestamp=None) -> [
        SavedJWLFLog]:

        search_key = f"{client}/{region}/{asset}/{folder}"
        current_inclusive_since_timestamp = self.search_since_timestamp_dict.get(search_key, inclusive_since_timestamp)

        stable_data_timestamp = self.get_stable_jwlf_data_timestamp(client, region, asset, folder)
        till_timestamp_exclusive = stable_data_timestamp + 1

        logs_without_data = self.get_jwlf_logs_without_data(client, region, asset, folder,
                                                            current_inclusive_since_timestamp,
                                                            till_timestamp_exclusive)
        ids = [log.union_id for log in logs_without_data]
        data = []
        if len(ids) > 0:
            data = self.get_jwlf_logs_with_data(client, region, asset, folder, ids)

        self.search_since_timestamp_dict[search_key] = till_timestamp_exclusive
        return data

    def get_jwlf_logs_with_data(self, client, region, asset, folder, ids) -> [SavedJWLFLog]:
        ids_batches = [[]]
        id_package_index = 0
        id_index_in_batch = 0
        for jwlf_id in ids:
            if id_index_in_batch >= self.__MAX_NUMBER_OF_IDS_PER_REQUEST__:
                id_package_index += 1
                ids_batches.append([])
                id_index_in_batch = 0
            ids_batches[id_package_index].append(jwlf_id)
            id_index_in_batch += 1

        saved_jwlf_log_dicts = []
        all_ids_num = len(ids)
        for ids_batch in ids_batches:
            query_params = {'id': ids_batch}
            url = f"{self.union_url}/api/v1/well-logs/{client}/{region}/{asset}/{folder}"
            access_token = self.__get_access_token__()

            response: Response = requests.get(
                url, params=query_params, headers={"Authorization": f"Bearer {access_token}"})

            if response.status_code >= 300:
                raise Exception(
                    f"UnionClient#get_new_jwlf_logs_with_data failure with"
                    f" http response status code={response.status_code}"
                    f" and response={response.text}")
            data_json = response.json()
            saved_jwlf_log_dicts.extend(data_json['list'])
            if len(ids_batches) > 1:
                logging.info(f"Receiving new output data packets {(len(saved_jwlf_log_dicts) / all_ids_num) * 100.0}%")

        return [SavedJWLFLog.from_dict(saved_jwlf_log_dict) for saved_jwlf_log_dict in saved_jwlf_log_dicts]

    def get_jwlf_logs_without_data(self, client, region, asset, folder, since_timestamp_inclusive=None,
                                   till_timestamp_exclusive=None) -> [SavedJWLFLog]:
        if not till_timestamp_exclusive:
            stable_data_timestamp = self.get_stable_jwlf_data_timestamp(client, region, asset, folder)
            till_timestamp_exclusive = stable_data_timestamp + 1

        url = f"{self.union_url}/api/v1/well-logs/{client}/{region}/{asset}/{folder}"
        query_params = {
            "sinceTimestamp": since_timestamp_inclusive,
            "tillTimestamp": till_timestamp_exclusive
        }
        access_token = self.__get_access_token__()
        response: Response = requests.get(
            url, params=query_params, headers={"Authorization": f"Bearer {access_token}"})
        if response.status_code >= 300:
            raise Exception(
                f"UnionClient#get_jwlf_logs_without_data failure with"
                f" http response status code={response.status_code}"
                f" and response={response.text}")

        response_dict = response.json()
        return [SavedJWLFLog.from_dict(saved_jwlf_log_dict) for saved_jwlf_log_dict in response_dict['list']]

    def get_stable_jwlf_data_timestamp(self, client, region, asset, folder) -> int:
        url = f"{self.union_url}/api/v1/well-logs/{client}/{region}/{asset}/{folder}"
        access_token = self.__get_access_token__()
        stable_timestamp_response: Response = requests.get(
            url, params={'sinceTimestamp': 0, 'tillTimestamp': 0}, headers={"Authorization": f"Bearer {access_token}"})
        if stable_timestamp_response.status_code >= 300:
            raise Exception(
                f"UnionClient#get_stable_jwlf_data_timestamp failure with"
                f" http response status code={stable_timestamp_response.status_code}"
                f" and response={stable_timestamp_response.text}")

        stable_timestamp_response_json = stable_timestamp_response.json()
        return stable_timestamp_response_json['stableDataTimestamp']

    def __get_access_token__(self):
        if self.access_token_creation_time:
            access_token_age_seconds = (datetime.now() - self.access_token_creation_time).total_seconds()
        else:
            access_token_age_seconds = UnionClient.__MAX_ACCESS_TOKEN_AGE_SECONDS__

        if access_token_age_seconds < UnionClient.__MAX_ACCESS_TOKEN_AGE_SECONDS__:
            return self.last_access_token
        url = f"{self.auth_provider_url}/auth/realms/{self.auth_provider_realm}/protocol/openid-connect/token"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        payload = {
            # "client_secret", self.client_secret_if_setup_in_auth_provider_for_client
            # "totp", totp_from_user_phone_if_2FA_is_setup
            "client_id": self.auth_client_id,
            "grant_type": "password",
            "username": self.username,
            "password": self.password
        }
        res = requests.post(url, data=payload, headers=headers)
        if res.status_code >= 300:
            raise Exception(
                f"Access token retrieval failure with http response status code={res.status_code} and response={res.text}")
        self.access_token_creation_time = datetime.now()
        self.last_access_token = res.json()["access_token"]
        return self.last_access_token
