# tap-successfactors
This tap successfactors was created by Degreed to be used for extracting data via Meltano into defined targets.

# Configuration required:

```python
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
```
## Testing locally

To test locally, pipx poetry
```bash
pipx install poetry
```

Install poetry for the package
```bash
poetry install
```

To confirm everything is setup properly, run the following: 
```bash
poetry run tap-successfactors --help
```

To run the tap locally outside of Meltano and view the response in a text file, run the following: 
```bash
poetry run tap-successfactors > output.txt 
```

A full list of supported settings and capabilities is available by running: `tap-successfactors --about`