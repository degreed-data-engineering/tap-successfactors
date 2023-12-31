"""Stream class for tap-successfactors."""

import datetime
import logging
import pandas as pd
import requests
import json
import time
import urllib.parse

from http import HTTPStatus
from pathlib import Path
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk import typing as th
from typing import Optional, Dict, Any, Iterable
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


def token_request(client_id, client_secret, base_url, user_id, company_id, userType):
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
    logging.info(f"Token created for user type: {userType}")
    access_token = "Bearer " + response.json()["access_token"]
    return access_token


class TapSuccessFactorsStream(RESTStream):
    """Generic SuccessFactors stream class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.columns = [
            "Feed",
            "ID",
            "Title",
            "Description",
            "thumbnailURI",
            "typeID",
            "revisionDate",
            "deliveryMethodID",
            "deliveryMethodDesc",
            "totalLength",
            "creditHours",
            "cpeHours",
            "active",
            "subjectAreaID",
            "subjectAreaDesc",
        ]

        self.results_df = pd.DataFrame(columns=self.columns)

        self.language = self.config["language"]
        self.target_user_id = self.config["target_user_id"]
        if not hasattr(self, "admin_token"):
            self.admin_token = token_request(
                self.config["client_id"],
                self.config["client_secret"],
                self.config["base_url"],
                self.config["user_id"],
                self.config["company_id"],
                "admin",
            )
        if not hasattr(self, "user_token"):
            self.user_token = token_request(
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

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Authorization"] = self.admin_token
        return headers

    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses."""
        full_path = urlparse(response.url).path or self.path
        error_type = (
            "Client"
            if HTTPStatus.BAD_REQUEST  # 400
            <= response.status_code
            < HTTPStatus.INTERNAL_SERVER_ERROR  # 500
            else "Server"
        )

        return (
            f"{response.status_code} {error_type} Error: "
            f"{response.reason} for path: {full_path}"
        )

    def validate_response(self, response):
        data = response.json()

        if (
            response.status_code == HTTPStatus.TOO_MANY_REQUESTS  # 429
            or HTTPStatus.INTERNAL_SERVER_ERROR  # 500
            <= response.status_code
            <= max(HTTPStatus)  # 511
        ):
            if (
                "message" in data["error"]
                and "No search results for provided search criteria"
                in data["error"]["message"]
            ):  # mainly used by ScheduledOfferings in case of no results found for search criteria
                logging.warning(
                    f"No search results for provided search criteria. URL: {urllib.parse.unquote(response.request.url)}"
                )
                pass
            else:
                msg = self.response_error_message(response)
                raise RetriableAPIError(msg, response)

        if (
            HTTPStatus.BAD_REQUEST  # 400
            <= response.status_code
            < HTTPStatus.INTERNAL_SERVER_ERROR  # 500
        ):
            if (
                "error_description" in data
                and data["error_description"] == "The token has expired."
            ):
                logging.warning("Token expired")
                if response.request.headers["Authorization"] == self.admin_token:
                    user_type = "admin"
                elif response.request.headers["Authorization"] == self.user_token:
                    user_type = "user"
                logging.warning(f"Refreshing {user_type} token")
                new_token = token_request(
                    self.config["client_id"],
                    self.config["client_secret"],
                    self.config["base_url"],
                    self.config["user_id"],
                    self.config["company_id"],
                    user_type,
                )
                if user_type == "admin":
                    self.admin_token = new_token
                elif user_type == "user":
                    self.user_token = new_token
                response.request.headers["Authorization"] = new_token
                msg = self.response_error_message(response)
                raise RetriableAPIError(msg, response)
            else:
                msg = self.response_error_message(response)
                raise FatalAPIError(msg)


class Catalogs(TapSuccessFactorsStream):
    name = "catalogs"
    path = "/learning/odatav4/public/admin/catalog-service/v1/Catalogs"
    primary_keys = ["ID"]
    records_jsonpath = "$[*]"
    schema = th.PropertiesList(
        th.Property("Feed", th.StringType),
        th.Property("ID", th.StringType),
        th.Property("Title", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("thumbnailURI", th.StringType),
        th.Property("typeID", th.StringType),
        th.Property("revisionDate", th.IntegerType),
        th.Property("deliveryMethodID", th.StringType),
        th.Property("deliveryMethodDesc", th.StringType),
        th.Property("totalLength", th.NumberType),
        th.Property("creditHours", th.NumberType),
        th.Property("cpeHours", th.NumberType),
        th.Property("active", th.BooleanType),
        th.Property("subjectAreaID", th.StringType),
        th.Property("subjectAreaDesc", th.StringType),
    ).to_dict()

    def _get_response(self, url):
        headers = {"Authorization": "{}".format(self.admin_token)}
        try:
            response = requests.request("GET", url, headers=headers)
            self.validate_response(response)
            return response
        except Exception as e:
            logging.error(e)

    def _get_catalogs_courses_feed(self, catalogId):
        url = f"{self.url_base}/learning/odatav4/public/admin/catalog-service/v1/CatalogsFeed('{catalogId}')/CoursesFeed?$filter=criteria/localeID eq '{self.language}'"
        response = self._get_response(url)

        for course in response.json()["value"]:
            subjectAreaID = None
            subjectAreaDesc = None
            if len(course["SubjectAreasFeed"]) > 0:
                if "subjectAreaID" in course["SubjectAreasFeed"][0]:
                    subjectAreaID = course["SubjectAreasFeed"][0]["subjectAreaID"]
                if "subjectAreaDesc" in course["SubjectAreasFeed"][0]:
                    subjectAreaDesc = course["SubjectAreasFeed"][0]["subjectAreaDesc"]
            new_row = pd.DataFrame(
                [
                    {
                        "Feed": "courses",
                        "ID": course["componentID"],
                        "Title": course["title"],
                        "Description": course["description"],
                        "thumbnailURI": course["thumbnailURI"],
                        "typeID": course["componentTypeID"],
                        "revisionDate": course["revisionDate"],
                        "deliveryMethodID": course["deliveryMethodID"],
                        "deliveryMethodDesc": course["deliveryMethodDesc"],
                        "totalLength": course["totalLength"],
                        "creditHours": course["creditHours"],
                        "cpeHours": course["cpeHours"],
                        "active": course["active"],
                        "subjectAreaID": subjectAreaID,
                        "subjectAreaDesc": subjectAreaDesc,
                    }
                ]
            )
            self.results_df = pd.concat(
                [self.results_df, new_row], axis=0, ignore_index=True
            )

    def _get_catalogs_curricula_feed(self, catalogId):
        url = f"{self.url_base}/learning/odatav4/public/admin/catalog-service/v1/CatalogsFeed('{catalogId}')/CurriculaFeed?$filter=criteria/localeID eq '{self.language}'"
        response = self._get_response(url)
        for curricula in response.json()["value"]:
            new_row = pd.DataFrame(
                [
                    {
                        "Feed": "curricula",
                        "ID": curricula["curriculumID"],
                        "Title": curricula["curriculumTitle"],
                        "Description": curricula["description"],
                        "thumbnailURI": curricula["thumbnailURI"],
                        "typeID": None,
                        "revisionDate": None,
                        "deliveryMethodID": None,
                        "deliveryMethodDesc": None,
                        "totalLength": None,
                        "creditHours": None,
                        "cpeHours": None,
                        "active": None,
                        "subjectAreaID": None,
                        "subjectAreaDesc": None,
                    }
                ]
            )
            self.results_df = pd.concat(
                [self.results_df, new_row], axis=0, ignore_index=True
            )

    def _get_catalogs_programs_feed(self, catalogId):
        url = f"{self.url_base}/learning/odatav4/public/admin/catalog-service/v1/CatalogsFeed('{catalogId}')/ProgramsFeed?$filter=criteria/localeID eq '{self.language}'"
        response = self._get_response(url)
        for program in response.json()["value"]:
            new_row = pd.DataFrame(
                [
                    {
                        "Feed": "programs",
                        "ID": program["programID"],
                        "Title": program["programTitle"],
                        "Description": program["description"],
                        "thumbnailURI": program["thumbnailURI"],
                        "typeID": None,
                        "revisionDate": None,
                        "deliveryMethodID": None,
                        "deliveryMethodDesc": None,
                        "totalLength": None,
                        "creditHours": None,
                        "cpeHours": None,
                        "active": None,
                        "subjectAreaID": None,
                        "subjectAreaDesc": None,
                    }
                ]
            )
            self.results_df = pd.concat(
                [self.results_df, new_row], axis=0, ignore_index=True
            )

    def _get_catalogs(self, response):
        for catalogId in response.json()["value"]:
            catalogId = catalogId["catalogID"]
            logging.info(f"Getting catalogId: {catalogId}")
            self._get_catalogs_courses_feed(catalogId)
            self._get_catalogs_curricula_feed(catalogId)
            self._get_catalogs_programs_feed(catalogId)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""

        if response.json():
            self._get_catalogs(response)
            input = self.results_df.to_dict(orient="records")
            for row in input:
                yield from extract_jsonpath(self.records_jsonpath, input=row)


# class Catalogs(TapSuccessFactorsStream):
#     name = "catalogs"
#     primary_keys = ["catalogID"]
#     records_jsonpath = "$.value[0:]"
#     path = "/learning/odatav4/public/admin/catalog-service/v1/Catalogs"
#     schema_filepath = SCHEMAS_DIR / "catalogs.json"

#     def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
#         """Return a context dictionary for child streams."""
#         return {"catalog_name": record["catalogID"]}


# class CatalogsCoursesFeed(TapSuccessFactorsStream):
#     parent_stream_type = Catalogs
#     name = "catalogs_courses_feed"
#     primary_keys = ["componentID"]
#     records_jsonpath = "$.value[0:]"
#     schema_filepath = SCHEMAS_DIR / "catalogs_courses_feed.json"

#     @property
#     def path(self) -> str:
#         """Return API URL path component for stream."""
#         main_path = "/learning/odatav4/public/admin/catalog-service/v1/CatalogsFeed('{catalog_name}')/CoursesFeed"
#         filters = f"?$filter=criteria/localeID eq '{self.language}'"
#         return main_path + filters

#     def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
#         """Return a context dictionary for child streams."""
#         return {
#             "componentID": record["componentID"],
#             "componentTypeID": record["componentTypeID"],
#             "revisionDate": record["revisionDate"],
#         }


# class CatalogsCurriculaFeed(TapSuccessFactorsStream):
#     parent_stream_type = Catalogs
#     name = "catalogs_curricula_feed"
#     primary_keys = ["curriculumID"]
#     records_jsonpath = "$.value[0:]"
#     schema_filepath = SCHEMAS_DIR / "catalogs_curricula_feed.json"

#     @property
#     def path(self) -> str:
#         """Return API URL path component for stream."""
#         main_path = "/learning/odatav4/public/admin/catalog-service/v1/CatalogsFeed('{catalog_name}')/CurriculaFeed"
#         filters = f"?$filter=criteria/localeID eq '{self.language}'"
#         return main_path + filters


# class CatalogsProgramsFeed(TapSuccessFactorsStream):
#     parent_stream_type = Catalogs
#     name = "catalogs_programs_feed"
#     primary_keys = ["programID"]
#     records_jsonpath = "$.value[0:]"
#     schema_filepath = SCHEMAS_DIR / "catalogs_programs_feed.json"

#     @property
#     def path(self) -> str:
#         """Return API URL path component for stream."""
#         main_path = "/learning/odatav4/public/admin/catalog-service/v1/CatalogsFeed('{catalog_name}')/ProgramsFeed"
#         filters = f"?$filter=criteria/localeID eq '{self.language}'"
#         return main_path + filters


# class LearningHistorys(TapSuccessFactorsStream):
#     name = "learning_historys"
#     primary_keys = ["componentID"]
#     records_jsonpath = "$.value[0:]"
#     schema_filepath = SCHEMAS_DIR / "learning_historys.json"
#     replication_key = "fromDate"

#     @property
#     def path(self) -> str:
#         """Return API URL path component for stream."""
#         main_path = (
#             "/learning/odatav4/public/user/userlearning-service/v1/learninghistorys"
#         )

#         self.to_date = int(time.time())

#         if "from_date" in self.config:
#             self.from_date = self.config["from_date"]
#         else:
#             if "replication_key_value" in self.stream_state:
#                 self.from_date = self.stream_state["replication_key_value"]
#             else:
#                 self.from_date = 1325376000  # 2012-01-01 00:00:00 UTC

#         from_date_ts = datetime.datetime.utcfromtimestamp(self.from_date).strftime(
#             "%Y-%m-%d %H:%M:%S"
#         )
#         logging.info("Filtering 'learninghistorys' with from_date = %s", from_date_ts)

#         filters = f"?$filter=criteria/targetUserID eq '{self.target_user_id}' and criteria/fromDate eq {self.from_date} &$count=true"
#         return main_path + filters

#     def post_process(self, row: dict, context: Optional[dict]) -> dict:
#         row["fromDate"] = self.to_date
#         return row


# class ScheduledOfferings(TapSuccessFactorsStream):
#     parent_stream_type = CatalogsCoursesFeed
#     name = "scheduled_offerings"
#     primary_keys = ["scheduleID"]
#     records_jsonpath = "$.value[0:]"
#     path = "/learning/odatav4/public/user/learningplan-service/v1/Scheduledofferings?$filter=lisCriteria/itemID eq '{componentID}' and lisCriteria/itemTypeID eq '{componentTypeID}' and lisCriteria/revisionDate eq {revisionDate}"
#     schema_filepath = SCHEMAS_DIR / "scheduled_offerings.json"

#     @property
#     def http_headers(self) -> dict:
#         """Return the http headers needed."""
#         headers = {}
#         headers["Authorization"] = f"{self.user_token}"
#         return headers


# class UserTodoLearningItems(TapSuccessFactorsStream):
#     name = "user_todo_learning_items"
#     primary_keys = ["sku"]
#     records_jsonpath = "$.value[0:]"
#     schema_filepath = SCHEMAS_DIR / "user_todo_learning_items.json"

#     @property
#     def path(self) -> str:
#         """Return API URL path component for stream."""
#         main_path = "/learning/odatav4/public/user/learningplan-service/v1/UserTodoLearningItems"
#         filters = f"?$filter=criteria/maxRowNum eq 999999 and criteria/targetUserID eq '{self.target_user_id}'"
#         return main_path + filters
