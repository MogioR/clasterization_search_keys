# установка
# 1) pip3 install --upgrade google-api-python-client
# 2) pip3 install oauth2client

# адрес сервисного аккаунта ts-234@task-8-308209.iam.gserviceaccount.com
# гайд https://habr.com/ru/post/483302/


import os.path
from utils.file import load_txt, save_txt
import time
from typing import Any, Dict, List, Tuple

import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from utils.file import csv_convert_json

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

class Cells:
    def __init__(self, r1:int = 0, c1:int = 0, r2:Any = None, c2:Any = None, color:tuple = (0.2, 0.7, 0.2), mode = False) -> None:
        self.r1 = r1
        self.r2 = r2 if r2 else r1 + 1
        self.c1 = c1 
        self.c2 = c2 if c2 else c1 + 1
        if mode:
            self.color = [color[0]/255, color[1]/255, color[2]/255]
        else:
            self.color = color

    def get_range(self, sheetId) -> dict:
        return {
            "sheetId":sheetId,
            "startRowIndex": self.r1,
            "endRowIndex": self.r2,
            "startColumnIndex": self.c1,
            "endColumnIndex": self.c2
        }

    
    def get_color(self) -> dict:
        return {
            "red": self.color[0],  
            "green": self.color[1],
            "blue": self.color[2],
            "alpha": 1
        }


class Table:
    sheetId:int = 0
    sheet_title:str = None

    def __init__(self, spreadsheetId:str = None, title:str = 'Новая таблица'):
        if spreadsheetId:
            spreadsheetId = spreadsheetId.replace('https://docs.google.com/spreadsheets/d/', '')
            i = spreadsheetId.find('/')
            if i != -1:spreadsheetId = spreadsheetId[:i]
        self.service = self.get_service()
        if spreadsheetId is None:spreadsheetId = self.create_table(title = title)
        self.spreadsheetId:int = spreadsheetId
        self.title, self.sheet_list = self.get_sheets()
        #print(self.sheet_list)
    
    def load_base_sheet(self) -> Tuple[dict]:
        params = {
            'gid':self.sheetId,
            'exportFormat':'csv'
        }
        return csv_convert_json(requests.get(f'https://docs.google.com/spreadsheets/d/{self.spreadsheetId}/export', params = params).content.decode('UTF-8'))
    
    def select_sheet(self, sheet_title):
        if self.sheet_title == sheet_title: return
        self.sheet_title = sheet_title
        self.sheetId = self.sheet_list.get(sheet_title)
        if self.sheetId is None: self.sheetId = self.create_sheet(sheet_title)

    def clear_sheet(self, sheet_title):
        if sheet_title in self.sheet_list:
            self.select_sheet(sheet_title)
            self.delete_sheet()
            time.sleep(0.1)
        self.select_sheet(sheet_title)
    
    def clear_sheet2(self):
        rangeAll = '{0}!A1:Z'.format(self.sheet_title)
        body = {
            "requests": [
                {
                    "updateCells": {
                        "range": {
                            "sheetId": self.sheetId
                        },
                        "fields": "userEnteredFormat"
                    }
                }
            ]
        }
        self.service.spreadsheets().batchUpdate(spreadsheetId = self.spreadsheetId, body = body).execute()
        self.service.spreadsheets().values().clear(spreadsheetId = self.spreadsheetId, range = rangeAll, body = {}).execute()

    def get_service(self):
        creds = None
        if os.path.exists('token_table.json'):
            creds = Credentials.from_authorized_user_file('token_table.json', SCOPES)
        if not creds: # or not creds.valid
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            with open('token_table.json', 'w') as token:
                token.write(creds.to_json())

        return build('sheets', 'v4', credentials=creds)
    
    def get_table_info(self):
        request = self.service.spreadsheets().get(spreadsheetId=self.spreadsheetId, ranges=[], includeGridData=False)
        return request.execute()

    @property
    def url(self) -> str:
        return f'https://docs.google.com/spreadsheets/d/{self.spreadsheetId}'

    def get_sheets(self) -> Tuple[str, Dict[str, int]]:
        sheet_list = dict()
        r = self.get_table_info()
        title = r['properties']['title']
        for sheet in r['sheets']:
            properties = sheet['properties']
            sheet_list.update({properties['title']:properties['sheetId']})
        return title, sheet_list

    def create_table(self, title):
        spreadsheet = self.service.spreadsheets().create(body = {
            'properties': {'title': title, 'locale': 'ru_RU'},
            'sheets': [{'properties': {'sheetType': 'GRID',
                                    'sheetId': 0,
                                    'title': 'Новый лист',
                                    'gridProperties': {'rowCount': 1, 'columnCount': 1}}}]
        }).execute()
        self.spreadsheetId = spreadsheet['spreadsheetId']  
        print(self.url)
        table_urls = load_txt('table_urls') or list()
        table_urls.append(self.url)
        save_txt(table_urls, 'table_urls')
        return self.spreadsheetId# сохраняем идентификатор файла
    
    def create_sheet(self, sheet_title):
        if sheet_title in self.sheet_list.keys(): return False
        results = self.service.spreadsheets().batchUpdate(
        spreadsheetId = self.spreadsheetId,
        body = {
            "requests": [
                {
                "addSheet": {
                    "properties": {
                    "title": sheet_title,
                    "gridProperties": {
                        "rowCount": 100,
                        "columnCount": 30
                    }
                    }
                }
                }
            ]
            }).execute() #'replies': [{'addSheet': {'properties': {'sheetId
        sheet_id = results['replies'][0]['addSheet']['properties']['sheetId']
        self.sheet_list.update({sheet_title:sheet_id})
        return sheet_id

    def delete_sheet(self):
        body = {
            'requests': [
                    {"deleteSheet": {"sheetId": self.sheetId}}
            ]
        }
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheetId,
            body=body).execute()
        for key, id_ in self.sheet_list.items():
            if id_ == self.sheetId:break
        self.sheet_list.pop(key)
        self.sheet_title = None
                

    def delete_sheet_all(self):
        self.select_sheet('1')
        time.sleep(0.5)
        self.select_sheet('0')
        self.delete_sheet()
        time.sleep(0.5)
        self.select_sheet('0')
        self.select_sheet('1')
        time.sleep(0.5)
        self.delete_sheet()
        time.sleep(0.5)
        for sheet_title in list(self.sheet_list.keys()):
            if sheet_title == '0': continue
            self.select_sheet(sheet_title)
            print('удаляем лист:', sheet_title)
            self.delete_sheet()
            time.sleep(0.5)

    def update_values(self, data, row = 1, col = 1, new_table = False):
        #print(list_range)
        if len(data) == 0: return row
        list_range = f'{chr(64 + col)}{row}:{chr(64 + len(data[0]))}{len(data) + row}'
        try:
            result = self.service.spreadsheets().values().batchUpdate(spreadsheetId = self.spreadsheetId, body = {
                "valueInputOption": "USER_ENTERED", # Данные воспринимаются, как вводимые пользователем (считается значение формул)
                "data": [
                    {
                        "range": f'{self.sheet_title}!{list_range}', 
                        "majorDimension": "ROWS",     # Сначала заполнять строки, затем столбцы
                        "values": data
                    }
                ]
            }).execute()
        except Exception as e:
            print(e)
            if new_table:
                print('Какая то ошибка, создал таблицу, но не смог туда загрузить')
                raise
            sheet_title = self.sheet_title
            self.create_table(title = self.title)
            self.title, self.sheet_list = self.get_sheets()
            self.select_sheet(sheet_title)
            time.sleep(1)
            return self.update_values(data, row = 1, col = 1, new_table = True)
            
        time.sleep(1)
        return len(data) + row - 1
    
    def set_size_colomn(self):
        self.service.spreadsheets().batchUpdate(spreadsheetId = self.spreadsheetId, body = {
            "requests": [
                {
                    "updateDimensionProperties": {
                        "range": {
                        "sheetId": self.sheetId,
                        "dimension": "COLUMNS",  # Задаем ширину колонки
                        "startIndex": 0, # Нумерация начинается с нуля
                        "endIndex": 2 # Со столбца номер startIndex по endIndex - 1 (endIndex не входит!)
                        },
                        "properties": {
                        "pixelSize": 250 # Ширина в пикселях
                        },
                        "fields": "pixelSize" # Указываем, что нужно использовать параметр pixelSize  
                    }
                },
            ]}
        ).execute()
    
    def set_format_Cell(self, cells:List[Cells]):
        if not len(cells): return
        requests = list()
        for cell in cells:
            setting_cell = {
                "repeatCell": 
                {
                    "cell": 
                    {
                    "userEnteredFormat": 
                    {
                        "backgroundColor": cell.get_color(),
                    }
                    },
                    "range":cell.get_range(sheetId = self.sheetId),
                    "fields": "userEnteredFormat"
                }
            }
            requests.append(setting_cell)
        self.service.spreadsheets().batchUpdate(spreadsheetId = self.spreadsheetId, body = {"requests": requests}).execute()

    def set_frozen_row(self):
        self.service.spreadsheets().batchUpdate(spreadsheetId = self.spreadsheetId, body = {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": self.sheetId,
                            "gridProperties":{
                                "frozenRowCount":1
                            }
                        },
                        "fields":"gridProperties.frozenRowCount"
                    }
                },
            ]}
        ).execute()

if __name__ == '__main__':
    table = Table('https://docs.google.com/spreadsheets/d/1qglHvDpBOXEJk4YRwZOUxCfHJrjS0XMTF_zU4Sd2LfA/edit#gid=1918215298') #main() №'1utinFVgu39rsTpZRKim5lsqtquFRdsFG2EXYQFyC5hg'
    print(table.title)
    table.create_sheet('test')
    #table.select_sheet('temp')
    #table.set_format_Cell2([Cells()])
