from tkinter import S
from omeka_s_tools.api import OmekaAPIClient
import pandas as pd
from pprint import pprint
from copy import deepcopy
from tqdm import tqdm
import os

class Task:
    term = "dcterms:identifier"

    def __init__(self, API_URL, KEY_IDENTITY, KEY_CREDENTIAL):
        self.API_URL = API_URL
        self.KEY_IDENTITY = KEY_IDENTITY
        self.KEY_CREDENTIAL = KEY_CREDENTIAL

    def reset(self):
        self.omeka_auth = OmekaAPIClient(
            api_url=self.API_URL,
            key_identity=self.KEY_IDENTITY, 
            key_credential=self.KEY_CREDENTIAL
        )

    def upload(self, metadata_path, media_path, is_public):
        '''アイテムとメディアの登録
        '''

        self.is_public = is_public

        # アイテムの登録
        self.upload_items(metadata_path)        

        # メディアの登録
        items = self.convertCsv2Json(media_path)
        self.upload_media(items, media_path)

    def upload_items(self, metadata_path):
        '''アイテムの登録
        '''
        print("アイテムの登録")
        items = self.convertCsv2Json(metadata_path)
        for item in tqdm(items):
            self.update(item)

    def convertCsv2Json(self, csv_path):
        df = pd.read_csv(csv_path)
        # print(df)

        items = []
        for index, row in df.iterrows():
            test_item = {}
            for column_name, item in df.iteritems():
                if column_name not in test_item:
                    test_item[column_name] = []
                value = row[column_name]
                if pd.isnull(value):
                    continue
                test_item[column_name].append(value)

            # pprint(test_item)
            items.append(test_item)
            # break

        # pprint(items)

        return items

    def getExistingsValues(self, term, value):
        url = f"{self.API_URL}/items?property[0][property]={term}&property[0][type]=eq&property[0][text]={value}&key_identity={self.KEY_IDENTITY}&key_credential={self.KEY_CREDENTIAL}"
        import requests
        df = requests.get(url).json()
        return df

    def update(self, test_item):

        term = self.term

        self.reset()
        omeka_auth = self.omeka_auth

        local_item = deepcopy(test_item)

        replaced_fields = {}

        for field in local_item:
            if "^^resource" in field:
                values = local_item[field]

                resources = []

                for value in values:
                    existing_values = self.getExistingsValues(term, value)

                    if len(existing_values) > 0:
                        resources.append({
                            "type": "resource",
                            "value": existing_values[0]["o:id"]
                        })

                '''
                del local_item[field]
                local_item[field.split(" ")[0]] = resources
                '''
                replaced_fields[field] = resources

        for field in replaced_fields:
            del local_item[field]
            local_item[field.split(" ")[0]] = replaced_fields[field]

        payload = omeka_auth.prepare_item_payload(local_item)
        
        # 既存のIDを取得
        id = local_item[term][0]

        # 既存のIDを持つアイテムの取得
        results = self.getExistingsValues(term, id)

        flg = True

        if flg:

            # 既に存在する場合は更新
            if len(results) > 0:
                payload_old = results[0]
                for key in payload:
                    payload_old[key] = payload[key]

                payload = payload_old

        payload["is_public"] = self.is_public

        # バグ修正

        for field in payload:
            values = payload[field]
            if type(values) is not list:
                continue
            for value in values:
                # @value を value_resource_id に置換する
                if "type" in value and value["type"] == "resource" and "@value" in value:
                    value["value_resource_id"] = value["@value"]

        if len(results) > 0:
            omeka_auth.update_resource(payload, 'items')
        else:
            omeka_auth.add_item(payload)

    def upload_media(self, items, media_path):
        dir = os.path.dirname(media_path)
        id_map = {}
        term = self.term

        print("\nIDの取得\n")

        for item in tqdm(items):
            self.reset()

            id = item["item"][0]

            if id in id_map:
                continue

            arr = self.getExistingsValues(term, id)

            if len(arr) > 0:
                id_map[id] = arr[0]["o:id"]

        print("\nメディアの登録\n")

        for item in tqdm(items):
            id = item["item"][0]

            if id not in id_map:
                continue

            media_path = f'{dir}/{item["path"][0]}'
            oid = id_map[id]

            self.reset()
            self.omeka_auth.add_media_to_item(oid, media_path)