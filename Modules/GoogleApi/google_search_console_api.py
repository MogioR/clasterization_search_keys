import httplib2
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
import datetime
from dateutil.relativedelta import *

# Report 16 month earlier
COUNT_OF_DAYS_STATISTIC = 30 * 16


class GoogleSearchConsoleApi:
    def __init__(self, token):
        self.auth_service = None
        self.authorization(token)

    # Authorisation in serves google
    # Accept: authorisation token
    def authorization(self, token: str):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            token,
            ['https://www.googleapis.com/auth/webmasters.readonly'])
        http_auth = credentials.authorize(httplib2.Http())
        self.auth_service = discovery.build('searchconsole', 'v1', http=http_auth)

    # Execute request to domain by request
    def execute_request(self, domain: str, request: dict):
        return self.auth_service.searchanalytics().query(siteUrl=domain, body=request).execute()

    # Return list of [key, clicks, impressions]
    def get_keys_by_url(self, domain: str, root: str, start_date: datetime = None, end_date: datetime = None):
        if start_date is None or end_date is None:
            end_date = datetime.date.today() - relativedelta(days=1)
            start_date = end_date - relativedelta(days=COUNT_OF_DAYS_STATISTIC)

        maxRows = 25000  # Maximum 25K per call
        numRows = 0  # Start at Row Zero

        request = {
            'startDate': datetime.datetime.strftime(start_date, '%Y-%m-%d'),
            'endDate': datetime.datetime.strftime(end_date, '%Y-%m-%d'),
            'rowLimit': maxRows,
            'startRow': numRows,
            "dimensions": [
                "query"
            ],
            "dimensionFilterGroups": [
                {
                    "filters": [
                        {
                            "dimension": "PAGE",
                            "operator": "EQUALS",
                            "expression": domain+root
                        }
                    ]
                }
            ]
        }
        results = []

        try:
            responce = self.execute_request(domain, request)
            for row in responce['rows']:
                results.append([str(row['keys'][0]), int(row['clicks']), int(row['impressions'])])
        except Exception as e:
            raise Exception("Can't download stats. Error:\n"+str(e))

        return results
