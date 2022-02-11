import httplib2
import time
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials


class GoogleSheetsApi:
    def __init__(self, token):
        self.auth_service = None
        self.request_count = 0
        self.request_limit = 60
        self.request_sleep = 200
        self.authorization(token)

    # Authorisation in serves google
    # Accept: authorisation token
    def authorization(self, token):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            token,
            ['https://www.googleapis.com/auth/spreadsheets'])
        http_auth = credentials.authorize(httplib2.Http())
        self.auth_service = discovery.build('sheets', 'v4', http=http_auth)
        self.request_count += 1

    # Get data from document table_id, sheet list_name in range [start_range_point, end_range_point]
    # Return: mas with data with major_dimension (ROWS/COLUMNS)
    def get_data_from_sheets(self, table_id: str, list_name: str, start_range_point: str,
                             end_range_point: str, major_dimension: str):
        self.request_count += 1
        if self.request_count >= self.request_limit:
            self.request_count = 0
            time.sleep(self.request_sleep)

        values = self.auth_service.spreadsheets().values().get(
            spreadsheetId=table_id,
            range="'{0}'!{1}:{2}".format(list_name, start_range_point, end_range_point),
            majorDimension=major_dimension
        ).execute()

        return values['values']

    # Put data to document table_id, sheet list_name in range [start_range_point, end_range_point]
    # in major_dimension (ROWS/COLUMNS)
    def put_data_to_sheets(self, table_id: str, list_name: str, start_range_point: str, end_range_point: str,
                           major_dimension: str, data: list):

        self.request_count += 1
        if self.request_count >= self.request_limit:
            self.request_count = 0
            time.sleep(self.request_sleep)

        values = self.auth_service.spreadsheets().values().batchUpdate(
            spreadsheetId=table_id,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": [{
                    "range": ("{0}!{1}:{2}".format(list_name, start_range_point, end_range_point)),
                    "majorDimension": major_dimension,
                    "values": data
                }]
            }).execute()

    # Put data to document table_id, sheet list_name in column column(char) and range [start_row, start_row+len(data)]
    def put_column_to_sheets(self, table_id: str, list_name: str, column: str, start_row: int, data: list):
        values = [[i] for i in data]
        self.put_data_to_sheets(table_id, list_name, column + str(start_row),
                           column + str(start_row+len(data)), 'ROWS', values)

    # Put data to document table_id, sheet list_name in row column and
    # range [start_column(char), start_column+len(data)]
    def put_row_to_sheets(self, table_id: str, list_name: str, row: int, start_column: str, data: list):
        values = [[i] for i in data]
        end_column = self.convert_column_index_to_int(start_column) + len(data)
        end_column = self.convert_column_index_to_char(end_column)
        self.put_data_to_sheets(table_id, list_name, start_column + str(row), end_column + str(row), 'COLUMNS', values)

    # Get sheet_id of list_name in document table_id"""
    def get_sheet_id(self, table_id: str, list_name: str):
        self.request_count += 1
        if self.request_count >= self.request_limit:
            self.request_count = 0
            time.sleep(self.request_sleep)

        spreadsheet = self.auth_service.spreadsheets().get(spreadsheetId=table_id).execute()
        sheet_id = None
        for _sheet in spreadsheet['sheets']:
            if _sheet['properties']['title'] == list_name:
                sheet_id = _sheet['properties']['sheetId']
        return sheet_id

    # Generate spreadsheets request for colorizing range in table
    # Accept: document table_id, sheet list_name, start_column(int), start_row(int), end_column(int), end_row(int)
    #   color([r,g,b,a] r,g,b,a = [0;1])
    def gen_colorizing_range_request(self, table_id, list_name, start_column, start_row, end_column,
                                     end_row, color):
        return {
            "repeatCell": {
                "range": {
                    "sheetId": self.get_sheet_id(table_id, list_name),
                    "startRowIndex": start_row - 1,
                    "endRowIndex": end_row,
                    "startColumnIndex": start_column - 1,
                    "endColumnIndex": end_column
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": color[0],
                            "green": color[1],
                            "blue": color[2]
                        }
                    }
                },
                "fields": "userEnteredFormat.backgroundColor"
            }
        }

    # Generate spreadsheets request for auto resize columns
    def gen_auto_resize_column_request(self, table_id, list_name, start_column, end_column):
        return {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": self.get_sheet_id(table_id, list_name),
                    "dimension": "COLUMNS",
                    "startIndex": start_column - 1,
                    "endIndex": end_column
                }
            }
        }

    # Apply spreadsheets requests on document table_id
    def apply_spreadsheets_requests(self, table_id, requests):
        self.request_count += 1
        self.auth_service.spreadsheets().batchUpdate(
            spreadsheetId=table_id,
            body={"requests": [requests]}).execute()

    # Clear sheet list_name in document table_id
    def clear_sheet(self, table_id, list_name):
        self.request_count += 1
        if self.request_count >= self.request_limit:
            self.request_count = 0
            time.sleep(self.request_sleep)

        range_all = '{0}!A1:Z'.format(list_name)
        self.auth_service.spreadsheets().values().clear(spreadsheetId=table_id, range=range_all, body={}).execute()

    # Get sizes of sheet list_name in document table_id"""
    # Return [column_count, row_count]
    def get_list_size(self, table_id, list_name):
        self.request_count += 1
        if self.request_count >= self.request_limit:
            self.request_count = 0
            time.sleep(self.request_sleep)

        request = self.auth_service.spreadsheets().get(spreadsheetId=table_id, ranges=list_name).execute()
        return [request['sheets'][0]['properties']['gridProperties']['columnCount'],
                request['sheets'][0]['properties']['gridProperties']['rowCount']]

    # Convert char column_index to int
    # Accept: char column_index
    # Return: int column_index
    @staticmethod
    def convert_column_index_to_int(char_column_index):
        char_column_index = char_column_index.lower()
        int_index = 0
        len_ = len(char_column_index)-1
        for i in range(len_, -1, -1):
            digit = ord(char_column_index[i]) - ord('a') + 1
            int_index += digit * pow(26, len_ - i)

        return int_index

    # Convert int column_index to char
    # Accept: int column_index
    # Return: char column_index
    @staticmethod
    def convert_column_index_to_char(int_column_index):
        char_column_index = ''
        while int_column_index != 0:
            char_column_index += chr(ord('A') + int_column_index % 26 - 1)
            int_column_index //= 26

        return char_column_index[::-1]

    # Create new sheet in document
    # Accept: document_id and name of the new sheet
    def create_sheet(self, document_id, list_name, row_count=1000, column_count=26):
        self.request_count += 1
        if self.request_count >= self.request_limit:
            self.request_count = 0
            time.sleep(self.request_sleep)

        request = {
                    "addSheet": {
                        "properties": {
                            "title": list_name,
                            "gridProperties": {
                                "rowCount": row_count,
                                "columnCount": column_count
                            }
                        }
                    }
                }

        self.auth_service.spreadsheets().batchUpdate(spreadsheetId=document_id, body={"requests": [request]}).execute()

    # Delete sheet from document
    # Accept: document_id and name of the sheet to delete
    def delete_sheet(self, document_id, list_name):
        self.request_count += 1
        if self.request_count >= self.request_limit:
            self.request_count = 0
            time.sleep(self.request_sleep)

        request = {
                    "deleteSheet": {
                        "sheetId": self.get_sheet_id(document_id, list_name),
                    }
                }
        self.auth_service.spreadsheets().batchUpdate(spreadsheetId=document_id, body={"requests": [request]}).execute()

    # Add colorizing conditional formatting to document document_id, list list_name in rage start_column:start_row
    # end_column:end_row with rule type with value and color [0..1]
    def add_colorizing_conditional_formatting(self, document_id: str, list_name: str, start_column: int, start_row: int,
                                   end_column: int, end_row: int, color: list, type: str, value: str):
        request = {
            'addConditionalFormatRule': {
                'rule': {
                    'ranges': {
                        'sheetId': self.get_sheet_id(document_id, list_name),
                        'startRowIndex': start_row - 1,
                        'endRowIndex': end_row,
                        'startColumnIndex': start_column - 1,
                        'endColumnIndex': end_column
                    },
                    'booleanRule': {
                        'condition': {
                            'type': type,
                            'values': [{
                                'userEnteredValue': value
                            }]
                        },
                        'format': {
                            "backgroundColor": {
                                "red": color[0],
                                "green": color[1],
                                "blue": color[2]
                            }
                        }
                    }
                },
                'index': 0
            }
        }
        self.auth_service.spreadsheets().batchUpdate(spreadsheetId=document_id, body={"requests": [request]}).execute()

    # Create group in sheet list_name
    def create_group(self, document_id: str, list_name: str, start: int, end: int, dimension: str):
        self.request_count += 1
        if self.request_count >= self.request_limit:
            self.request_count = 0
            time.sleep(self.request_sleep)

        body = {
            "requests": [
                {
                    "addDimensionGroup": {
                        "range": {
                            "dimension": dimension,
                            "sheetId": self.get_sheet_id(document_id, list_name),
                            "startIndex": start,
                            "endIndex": end
                        }
                    }
                }
            ]
        }

        self.auth_service.spreadsheets().batchUpdate(spreadsheetId=document_id, body=body).execute()