"""Stream class for tap-successfactors."""

import logging
import requests
import json
import urllib.parse

from pathlib import Path
from typing import Dict, Optional, Any, Iterable
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIAuthenticatorBase, BasicAuthenticator

logging.basicConfig(level=logging.INFO)

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


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


class TapSuccessFactorsStream(RESTStream):
    """Generic SuccessFactors stream class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.language = self.config["language"]
        self.target_user_id = self.config["target_user_id"]
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
    schema_filepath = SCHEMAS_DIR / "catalogs.json"

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
    schema_filepath = SCHEMAS_DIR / "catalogs_courses_feed.json"

    @property
    def path(self) -> str:
        """Return API URL path component for stream."""
        main_path = "/learning/odatav4/public/admin/catalog-service/v1/CatalogsFeed('{catalog_name}')/CoursesFeed"
        filters = f"?$filter=criteria/localeID eq '{self.language}'"
        return main_path + filters

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Authorization"] = f"Bearer {self.admin_token}"
        return headers

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "componentID": record["componentID"],
            "componentTypeID": record["componentTypeID"],
            "revisionDate": record["revisionDate"],
        }


class CatalogsCurriculaFeed(TapSuccessFactorsStream):
    parent_stream_type = Catalogs
    name = "catalogs_curricula_feed"
    primary_keys = ["curriculumID"]
    records_jsonpath = "$.value[0:]"
    schema_filepath = SCHEMAS_DIR / "catalogs_curricula_feed.json"

    @property
    def path(self) -> str:
        """Return API URL path component for stream."""
        main_path = "/learning/odatav4/public/admin/catalog-service/v1/CatalogsFeed('{catalog_name}')/CurriculaFeed"
        filters = f"?$filter=criteria/localeID eq '{self.language}'"
        return main_path + filters

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
    schema_filepath = SCHEMAS_DIR / "catalogs_programs_feed.json"

    @property
    def path(self) -> str:
        """Return API URL path component for stream."""
        main_path = "/learning/odatav4/public/admin/catalog-service/v1/CatalogsFeed('{catalog_name}')/ProgramsFeed"
        filters = f"?$filter=criteria/localeID eq '{self.language}'"
        return main_path + filters

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Authorization"] = f"Bearer {self.admin_token}"
        return headers


class LearningHistorys(TapSuccessFactorsStream):
    name = "learning_historys"
    records_jsonpath = "$.value[0:]"
    schema_filepath = SCHEMAS_DIR / "learning_historys.json"

    @property
    def path(self) -> str:
        """Return API URL path component for stream."""
        main_path = (
            "/learning/odatav4/public/user/userlearning-service/v1/learninghistorys"
        )
        filters = (
            f"?$filter=criteria/targetUserID eq '{self.target_user_id}'&$count=true"
        )
        return main_path + filters

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Authorization"] = f"Bearer {self.admin_token}"
        return headers


class ScheduledOfferings(TapSuccessFactorsStream):
    parent_stream_type = CatalogsCoursesFeed
    name = "scheduled_offerings"
    records_jsonpath = "$.value[0:]"
    path = "/learning/odatav4/public/user/learningplan-service/v1/Scheduledofferings?$filter=lisCriteria/itemID eq '{componentID}' and lisCriteria/itemTypeID eq '{componentTypeID}' and lisCriteria/revisionDate eq {revisionDate}"
    schema_filepath = SCHEMAS_DIR / "scheduled_offerings.json"

    def validate_response(self, response):
        data = response.json()
        if "error" in data:
            if (
                "No search results for provided search criteria"
                in data["error"]["message"]
            ):
                logging.warning(
                    f"No search results for provided search criteria. URL: {urllib.parse.unquote(response.request.url)}"
                )
                pass

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Authorization"] = f"Bearer {self.user_token}"
        return headers


class UserTodoLearningItems(TapSuccessFactorsStream):
    name = "user_todo_learning_items"
    records_jsonpath = "$.value[0:]"
    schema_filepath = SCHEMAS_DIR / "user_todo_learning_items.json"

    @property
    def path(self) -> str:
        """Return API URL path component for stream."""
        main_path = "/learning/odatav4/public/user/learningplan-service/v1/UserTodoLearningItems"
        filters = f"?$filter=criteria/maxRowNum eq 999999 and criteria/targetUserID eq '{self.target_user_id}'"
        return main_path + filters

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Authorization"] = f"Bearer {self.admin_token}"
        return headers
