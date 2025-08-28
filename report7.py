import json
import gspread
from google.oauth2.service_account import Credentials

import os

class Report7:
    def __init__(self, CREDS, SCOPES):
        self.CREDS = CREDS
        self.SCOPES = SCOPES
        
    def __load_doc(self):
        print("loading doc")
        
        # Аутентификация с использованием учетных данных сервисного аккаунта
        service_account_info = json.loads(self.CREDS)
        creds = Credentials.from_service_account_info(service_account_info, scopes=self.SCOPES)
        
        # Создание клиента для работы с Google Sheets
        client = gspread.authorize(creds)
        
        # Открытие таблицы по ID
        sheet_id = os.getenv('MOA_TABLE_ID_DAILY')
        doc = client.open_by_key(sheet_id)

        return doc

    def __generate_misc(self, doc):
        # Получаем нужный лист по ID
        worksheet = doc.get_worksheet_by_id(19278)

        # Получаем все значения из колонки I, начиная с 3-й строки
        column_i_values = worksheet.col_values(2)[2:]  # 2 - это номер колонки B

        # Преобразуем каждое значение в строку и создаем список
        result = []
        for value in column_i_values:
            arr = str(value).split(" ")
            arr[0] = "<b>" + arr[0] + "</b>"
            result.append(" ".join(arr))
        result_str = "\n".join(result)
        
        return result_str

    def __generate_cases(self, doc):
        worksheet = doc.get_worksheet_by_id(1664869904)
        # Получаем все значения из колонки I, начиная с 3-й строки
        column_a_values = worksheet.col_values(1)[1:]
        last = len(column_a_values) + 1
        selected = worksheet.get_values(f"A2:F{last}")
        result = []
        for a in selected:
            case_num = "<b>" + a[0] + "</b>"
            case_req = a[1]
            bank = a[2]
            loc = a[3]
            desc = a[4]
            resp = a[5]
            
            if (case_req != ''):
                result.append(" ".join([case_num, bank, loc, desc]) + " (запрос " + case_req + " / " + resp + ")")
            else:
                result.append(" ".join([case_num, bank, loc, desc]) + " (" + resp + ")")

        # Преобразуем каждое значение в строку и создаем список
        result_str = "\n".join(result)
        
        return result_str

    def generate_report7(self):
        doc = self.__load_doc()
        report = {"·ON·FOOD": f"Разное\n\n{self.__generate_misc(doc)}",
                "Кейсы": f"Кейсы\n\n{self.__generate_cases(doc)}",
                }
        return report
