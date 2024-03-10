import logging
import requests
from typing import List
from llama_hub.tools.salesforce import SalesforceToolSpec


class Salesforce:

    def __init__(
        self,
        username: str,
        password: str,
        consumer_key: str,
        consumer_secret: str,
        domain: str,
        url: str = None,
    ):
        """
        Parameters:
            username (str): Salesforce username.
            password (str): Salesforce password.
            consumer_key (str): Salesforce consumer key.
            consumer_secret (str): Salesforce consumer secret.
            domain (str): Salesforce domain (domain.salesforce.com).
            url (str): Salesforce url to download files.
                Default value: https://pwc-me--sundev.sandbox.file.force.com/sfc/servlet.shepherd/version/download/
        """

        self.__username = username
        self.__password = password
        self.__consumer_key = consumer_key
        self.__consumer_secret = consumer_secret
        self.__domain = domain
        self._url = (
            url
            if url
            else "https://pwc-me--sundev.sandbox.file.force.com/sfc/servlet.shepherd/version/download/"
        )
        self.sf = SalesforceToolSpec(
            username=self.__username,
            password=self.__password,
            consumer_key=self.__consumer_key,
            consumer_secret=self.__consumer_secret,
            domain=self.__domain,
        )
        self.Records_ids = []

    def generate_access_token(self):
        payload = {
            "grant_type": "password",
            "client_id": self.__consumer_key,
            "client_secret": self.__consumer_secret,
            "username": self.__username,
            "password": self.__password,
        }
        access_token_url = "https://test.salesforce.com/services/oauth2/token"
        response = requests.post(access_token_url, data=payload)
        return response.json()["access_token"]

    def get_records_ids(
        self,
        From: str = None,
        Where: str = None,
        OrderBy: str = None,
        Limit: str = None,
    ):
        """
        Get all the records IDs using SOQL.

        Parameters:
            From (str): Salesforce object name.
            Where (str): Where clause.
            OrderBy (str): Order by clause.
            Limit (str): Limit clause.
        """

        self.Records_ids = []

        Select = "SELECT Id "
        From = f"FROM {From} " if From else ""
        Where = f"WHERE {Where} " if Where else ""
        OrderBy = f"ORDER BY {OrderBy} " if OrderBy else ""
        Limit = f"LIMIT {Limit}" if Limit else ""

        try:
            records = self.sf.execute_soql(f"{Select}{From}{Where}{OrderBy}{Limit}")
            for i in range(records["totalSize"]):
                self.Records_ids.append(records["records"][i]["Id"])
            return self.Records_ids

        except Exception as e:
            logging.exception(e)

    def get_record_by_id(self, id: str):
        """
        Use get request to download the file using the file ID.

        Parameters:
            id (str): Salesforce record ID.
        """

        full_url = f"{self._url}{id}"
        headers = {
            "Authorization": "Bearer " + self.generate_access_token(),
            "Content-Type": "application/json",
        }
        try:
            response = requests.get(full_url, headers=headers)
        except Exception as e:
            logging.exception(e)

        if response.status_code == 200:
            return response.content

        return None

    def get_metadata_by_id(
        self, id, metadata_fields: List[str] = None, From: str = None
    ):
        """
        Get record metadata by the record ID using SOQL.

        Parameters:
            id (str): Salesforce record ID.
            metadata_fields (list of strings): salesforce fields names.
            From (str): Salesforce object name.
        """
        print(f"metadata_fields: {metadata_fields} from (get_metadata_by_id)")
        Select = ", ".join(metadata_fields)
        Select = f"SELECT {Select} "
        From = f"FROM {From} " if From else ""
        Where = f"WHERE Id= '{id}'"
        try:
            record = self.sf.execute_soql(f"{Select}{From}{Where}")
            record = record["records"][0]
            extracted_data = {}

            for i in metadata_fields:
                extracted_data[i] = record[i]

            return extracted_data

        except Exception as e:
            logging.exception(e)

    def get_content_documents_link(
        self, knowledge_id: str, Select: str = None, From: str = None, Where: str = None
    ):
        SELECT = f"SELECT {Select} " if Select else ""
        FROM = f"FROM {From} " if From else ""
        WHERE = f"WHERE {Where}='{knowledge_id}'" if Where else ""
        try:
            links = self.sf.execute_soql(f"{SELECT}{FROM}{WHERE}")
            return links["records"]
        except Exception as e:
            logging.exception(e)

    def get_asset_by_Id(self, asset_id: str):
        """
        Get all the documents related to the asset.

        Parameters:
            asset_id (str): Salesforce asset ID.
        """

        link = self.get_content_documents_link(
            asset_id,
            Select="LinkedEntityId",
            From="ContentDocumentLink",
            Where="ContentDocumentId",
        )
        print(link[-1]["LinkedEntityId"])
        try:
            asset = self.sf.execute_soql(
                f"SELECT Id, Title, CreatedDate, Description__c, URL__c FROM Knowledge__Kav WHERE Id='{link[-1]['LinkedEntityId']}'"
            )
            print(asset["records"][-1]["Title"])
            return asset["records"][-1]
        except Exception as e:
            print(e)

    def get_related_files_ids(self, content_document_id: str):
        response = self.sf.execute_soql(
            f"SELECT Id FROM FROM ContentVersion WHERE ContentDocumentId='{content_document_id}'"
        )

    def get_attachments_in_assets(self, assets_dict):
        attachment_ids_dict = {}
        try:
            for asset in assets_dict:
                files_ids = self.sf.execute_soql(
                    f"SELECT ContentDocumentId FROM ContentDocumentLink WHERE LinkedEntityId='{asset['Id']}'"
                )
                if files_ids["totalSize"] > 0:
                    attachment_ids_dict[asset["Id"]] = [
                        record["ContentDocumentId"] for record in files_ids["records"]
                    ]
                else:
                    attachment_ids_dict[asset["Id"]] = ["0"]
        except Exception as e:
            logging.exception(e)
        print(attachment_ids_dict)
        return attachment_ids_dict
