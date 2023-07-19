"""successfactors tap class."""
from typing import List
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_successfactors.streams import (
    Catalogs,
    # CatalogsCoursesFeed,
    # CatalogsCurriculaFeed,
    # CatalogsProgramsFeed,
    # LearningHistorys,
    # ScheduledOfferings,
    # UserTodoLearningItems,
)

PLUGIN_NAME = "tap-successfactors"

STREAM_TYPES = [
    Catalogs,
    # CatalogsCoursesFeed,
    # CatalogsCurriculaFeed,
    # CatalogsProgramsFeed,
    # LearningHistorys,
    # ScheduledOfferings,
    # UserTodoLearningItems,
]


class TapSuccessfactors(Tap):
    """successfactors tap class."""

    name = "tap-successfactors"
    config_jsonschema = th.PropertiesList(
        th.Property("base_url", th.StringType, required=True, description="Base URL"),
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="Client ID (often equal to company_id)",
        ),
        th.Property(
            "client_secret", th.StringType, required=True, description="Client Secret"
        ),
        th.Property(
            "user_id",
            th.StringType,
            required=True,
            description="User ID (i.e. degreed_api)",
        ),
        th.Property(
            "company_id",
            th.StringType,
            required=True,
            description="Company ID (often equal to client_id)",
        ),
        th.Property(
            "language",
            th.StringType,
            required=True,
            description="Language (i.e. English)",
        ),
        th.Property(
            "target_user_id",
            th.StringType,
            required=True,
            description="Target user ID (i.e. sfadmin)",
        ),
        th.Property(
            "from_date",
            th.IntegerType,
            required=False,
            description="Datetime (specified in unix milliseconds) to use as the start of the date range for the tap",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        return streams


# CLI Execution:
cli = TapSuccessfactors.cli
