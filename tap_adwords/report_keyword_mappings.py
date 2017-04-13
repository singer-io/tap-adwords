# TODO: add commas
REPORT_KEYWORD_MAPPINGS = {"ADGROUP_PERFROMANCE_REPORT": frozenset(['campaignID', 'adGroupID', 'customerID', 'account', 'day']),
                           "AD_PERFORMANCE_REPORT": frozenset(['campaignID', 'adID', 'adGroupID', 'customerID', 'keywordID', 'account', 'day']),
                           "CRITERIA_PERFORMANCE_REPORT": frozenset(['campaignID', 'adGroupID', 'customerID', 'keywordID', 'day']),
                           "DISPLAY_KEYWORD_PERFORMANCE_REPORT": frozenset(['campaignID', 'adGroupID', 'customerID', 'keywordID', 'day']),
                           "GEO_PERFORMANCE_REPORT": frozenset(['countryTerritory', 'campaignID', 'metroArea', 'mostSpecificLocation', 'adGroupID', 'city', 'customerID', 'region', 'account', 'day', 'adGroup']),
                           "KEYWORDS_PERFORMANCE_REPORT": frozenset(['customerID', 'adGroupID', 'keywordID', 'account', 'adID'])}
