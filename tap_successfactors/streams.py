"""Stream class for tap-successfactors."""

import logging
import requests
import json

from typing import Dict, Optional, Any
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIAuthenticatorBase, BasicAuthenticator

logging.basicConfig(level=logging.INFO)


class CustomAuthenticator:
    def token_request(
        self, client_id, client_secret, base_url, user_id, company_id, userType
    ):
        auth_endpoint = "{}/learning/oauth-api/rest/v1/token".format(base_url)
        payload = json.dumps(
            {
                "grant_type": "client_credentials",
                "scope": {
                    "userId": user_id,
                    "companyId": company_id,
                    "userType": userType,
                    "resourceType": "learning_public_api",
                },
            }
        )
        response = requests.request(
            "POST",
            auth_endpoint,
            data=payload,
            auth=requests.auth.HTTPBasicAuth(client_id, client_secret),
        )
        return response.json()["access_token"]

    def user_token(self):
        return 0


class TapSuccessFactorsStream(RESTStream):
    """Generic SuccessFactors stream class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.language = self.config["language"]
        if not hasattr(self, "admin_token"):
            self.admin_token = CustomAuthenticator().token_request(
                self.config["client_id"],
                self.config["client_secret"],
                self.config["base_url"],
                self.config["user_id"],
                self.config["company_id"],
                "admin",
            )
        if not hasattr(self, "user_token"):
            self.user_token = CustomAuthenticator().token_request(
                self.config["client_id"],
                self.config["client_secret"],
                self.config["base_url"],
                self.config["user_id"],
                self.config["company_id"],
                "user",
            )

    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return self.config["base_url"]


class Catalogs(TapSuccessFactorsStream):
    name = "catalogs"
    primary_keys = ["catalogID"]
    records_jsonpath = "$.value[0:]"
    path = "/learning/odatav4/public/admin/catalog-service/v1/Catalogs"

    schema = th.PropertiesList(th.Property("catalogID", th.StringType)).to_dict()

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Authorization"] = f"Bearer {self.admin_token}"
        return headers

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"catalog_name": record["catalogID"]}


class CatalogsCoursesFeed(TapSuccessFactorsStream):
    parent_stream_type = Catalogs
    name = "catalogs_courses_feed"
    primary_keys = ["componentID"]
    records_jsonpath = "$.value[0:]"
    path = "/learning/odatav4/public/admin/catalog-service/v1/CatalogsFeed('{catalog_name}')/CoursesFeed?$filter=criteria/localeID eq '{self.language}'"

    schema = th.PropertiesList(th.Property("componentID", th.StringType)).to_dict()

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Authorization"] = f"Bearer {self.admin_token}"
        return headers


class CatalogsCurriculaFeed(TapSuccessFactorsStream):
    parent_stream_type = Catalogs
    name = "catalogs_curricula_feed"
    primary_keys = ["curriculumID"]
    records_jsonpath = "$.value[0:]"
    path = "/learning/odatav4/public/admin/catalog-service/v1/CatalogsFeed('{catalog_name}')/CurriculaFeed?$filter=criteria/localeID eq '{self.language}'"

    schema = th.PropertiesList(th.Property("curriculumID", th.StringType)).to_dict()

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Authorization"] = f"Bearer {self.admin_token}"
        return headers


class CatalogsProgramsFeed(TapSuccessFactorsStream):
    parent_stream_type = Catalogs
    name = "catalogs_programs_feed"
    primary_keys = ["programID"]
    records_jsonpath = "$.value[0:]"
    path = "/learning/odatav4/public/admin/catalog-service/v1/CatalogsFeed('{catalog_name}')/ProgramsFeed?$filter=criteria/localeID eq '{self.language}'"

    schema = th.PropertiesList(th.Property("programID", th.StringType)).to_dict()

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Authorization"] = f"Bearer {self.admin_token}"
        return headers
