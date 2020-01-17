# tap-adwords

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from the AdWords API
- Extracts the following resources from AdWords for a one or more accounts:
  - [Accounts / Managed Customer](https://developers.google.com/adwords/api/docs/reference/v201705/ManagedCustomerService.ManagedCustomer)
  - [Campaigns](https://developers.google.com/adwords/api/docs/reference/v201705/CampaignService.Campaign)
  - [Ad Groups](https://developers.google.com/adwords/api/docs/reference/v201705/AdGroupService.AdGroup)
  - [Ads](https://developers.google.com/adwords/api/docs/reference/v201705/AdGroupAdService.AdGroupAd)
  - [AdWords Reports](https://developers.google.com/adwords/api/docs/appendix/reports)
- Supports MCC AdWords accounts
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

### Install

We recommend using a virtualenv:

```bash
> virtualenv -p python3 venv
> source venv/bin/activate
> pip install tap-adwords
```

### Get Access to the AdWords API

To use the AdWords API, you must request and be granted access.
https://developers.google.com/adwords/api/docs/guides/signup

### Set up OAuth2 authentication

The Tap will need to access user data and contact Google services on your behalf. Authentication via OAuth2 allows your app to operate on behalf of your account. To enable your app to access the API, you need an OAuth2 client ID and client secret.

[Read Google's guide](https://developers.google.com/adwords/api/docs/guides/first-api-call#set_up_oauth2_authentication) on getting your OAuth2 client ID and client secret.

https://developers.google.com/adwords/api/docs/guides/first-api-call#set_up_oauth2_authentication

### Create the config file

The AdWords Tap will use the developer token and OAuth properties from the previous steps. Additionally you will need:

  **start_date** - an initial date for the Tap to extract AdWords data  
  **user_agent** - used in requests made to the AdWords API  
  **customer_ids** - A comma-separated list of AdWords account IDs to replicate data from

The following is an example of the required configuration

```json
{"developer_token": "",
 "oauth_client_id": "",
 "oauth_client_secret": "",
 "refresh_token": "",
 "start_date": "",
 "user_agent": "",
 "customer_ids": ""}
```

### Create a properties file

The properties file will indicate what streams and fields to replicate from the AdWords API. The Tap takes advantage of the Singer best practices for [schema discovery and property selection](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#schema-discovery-and-property-selection).

### [Optional] Create the initial state file

You can provide JSON file that contains a date for the streams to force the application to only fetch data newer than those dates. If you omit the file it will fetch all data for the selected streams.

```json
{"campaigns_12345":"2017-01-01T00:00:00Z",
 "CLICK_PERFORMANCE_REPORT_12345":"2017-01-01T00:00:00Z",
 "CRITERIA_PERFORMANCE_REPORT_12345":"2017-01-01T00:00:00Z"}
```

### Run the Tap

`tap-adwords -c config.json -p properties.json -s state.json`

## Metadata Reference

tap-adwords uses some custom metadata keys:

* `behavior` - The “fieldBehavior” value from the Google Adwords API. Either "attribute", "metric", or "segment". (discoverable)
* `fieldExclusions` - Indicates which other fields may not be selected when this field is selected. If you invoke the tap with selections that violate fieldExclusion rules, the tap will fail. (discoverable)
* `tap-adwords.report-key-properties` - Is user set and indicates which fields to output as "key_properties" in the stream's SCHEMA message. (nondiscoverable)


---

Copyright &copy; 2017 Stitch
