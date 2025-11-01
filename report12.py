import json
import os
import requests
import pandas as pd

import requests

import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pytz
from dateutil import parser

import gspread
from google.oauth2.service_account import Credentials


class Report12:
    def __init__(self, CREDS, SCOPES):
        self.CREDS = CREDS
        self.SCOPES = SCOPES

        self.group_id = {
            "Общие вопросы": 82518,
            "Кейсы_банки и НСПК": 83508,
            "Возвраты ·ON·PASS": 82521,
            "Корректировки ·ON·PASS": 82522,
            "Регистрации ·ON·PASS": 86531,
            "·ON·PASS (эскалация в Нейрон)": 83860,
            "·ON·FOOD (эскалация в Нейрон)": 92089,
            "·ON·TRACK (эскалация в Нейрон)": 92090,
            "•ON•TAXI (эскалация в Нейрон)": 94907,
            "•ON•PACK (эскалация в Нейрон)": 94401,
            "•ON•VIP": 96273,
            "Запрос по кейсу": 88994,
            "Изъятые вещи": 83197,
            "Забытые вещи (Победа)": 84599,
            "Забытые вещи (Аэрофлот)": 96750,
            "Sky Service": 93313,
            "Рассылка": 94155,
            "МИЛИ (эскалация в Нейрон)": 96988,
            "Запрос менеджеру": 97008,
            "Запрос в БЗ (не банковские кейсы)": 97009,
            "Звонок МОА": 97089
        }
        self.sched = self.__load_schedule()
        self.schedule = self.__get_schedule(self.sched)
        self.MOA_API_CASES = 'https://mileonair.omnidesk.ru/api/cases.json'

    def __get_moa_tickets_page(self, params):
        # Basic Authentication
        staff_email = os.getenv('MOA_EMAIL')
        api_key = os.getenv('MOA_API_KEY')

        # URL
        url = self.MOA_API_CASES

        # Headers
        headers = {
            'Content-Type': 'application/json'
        }

        # Make the request
        response = requests.get(
            url,
            auth=(staff_email, api_key),
            headers=headers,
            params=params
        )

        # Check response
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Error: {response.status_code}")
            print(response.text)

        return None

    def __get_data_counts(self):
        pages = '1', '2', '3', '4', '5'
        df = None
        all = []
        for page in pages:
            params = {
                'status': 'waiting',
                'page': page
            }
            data = self.__get_moa_tickets_page(params)
            if (data['total_count'] == 0):
                break
            del data['total_count']
            cases = [data[key]['case'] for key in data.keys()]
            df = pd.DataFrame(cases)
            all.append(df)

        df = pd.concat(all, ignore_index=True)
        needed_columns = ['case_id', 'subject', 'group_id']
        df = df[needed_columns]
        return df

    def __get_counts(self, df):
        # print(df)
        value_counts = df['group_id'].value_counts()
        counts = {
            'Визиты': value_counts.get(self.group_id['Корректировки ·ON·PASS'], 0) +
            value_counts.get(self.group_id['Регистрации ·ON·PASS'], 0),
            'Возвраты': value_counts.get(self.group_id['Возвраты ·ON·PASS'], 0),
            'Разное': value_counts.get(self.group_id['·ON·PASS (эскалация в Нейрон)'], 0) +
            value_counts.get(self.group_id['·ON·FOOD (эскалация в Нейрон)'], 0) +
            value_counts.get(self.group_id['·ON·TRACK (эскалация в Нейрон)'], 0) +
            value_counts.get(self.group_id['•ON•TAXI (эскалация в Нейрон)'], 0) +
            value_counts.get(self.group_id['•ON•PACK (эскалация в Нейрон)'], 0) +
            value_counts.get(self.group_id['•ON•VIP'], 0) +
            value_counts.get(self.group_id['МИЛИ (эскалация в Нейрон)'], 0) +
            value_counts.get(self.group_id['Запрос менеджеру'], 0) +
            value_counts.get(
                self.group_id['Запрос в БЗ (не банковские кейсы)'], 0),
            'Кейсы': value_counts.get(self.group_id['Кейсы_банки и НСПК'], 0) + value_counts.get(self.group_id['Запрос по кейсу'], 0)
        }
        counts['Все'] = counts['Визиты'] + counts['Возвраты'] + \
            counts['Разное'] + counts['Кейсы']
        return counts

    def __get_all_processed(self, period):
        # Получаем текущее время в UTC
        utc_now = datetime.now(pytz.UTC)

        # Рассчитываем временные метки
        utc_today_6am = utc_now.replace(
            hour=6, minute=0, second=0, microsecond=0).timestamp()
        utc_today_6pm = utc_now.replace(
            hour=18, minute=0, second=0, microsecond=0).timestamp()
        utc_yesterday_6pm = (utc_now - timedelta(days=1)).replace(
            hour=18, minute=0, second=0, microsecond=0
        ).timestamp()

        pages = ['1', '2', '3', '4', '5']
        all_dfs = []  # Список для хранения DataFrame
        df = None

        for page in pages:
            params = {
                'page': page,
                'sort': 'response_desc',
            }
            # Получаем данные с помощью кастомного метода
            data = self.__get_moa_tickets_page(params)

            # Удаляем ненужное поле
            del data['total_count']

            # Формируем DataFrame из данных
            cases = [data[key]['case'] for key in data.keys()]
            df = pd.DataFrame(cases)

            # Получаем временную метку последнего ответа
            last = parser.parse(df.loc[len(df) - 1]
                                ['last_response_at']).timestamp()

            # Добавляем DataFrame в список
            all_dfs.append(df)

            # Условие завершения цикла в зависимости от периода
            if period == 'day' and last < utc_today_6am:
                break
            if period == 'night' and last < utc_yesterday_6pm:
                break

        # Объединяем все DataFrame в один
        df = pd.concat(all_dfs, ignore_index=True)

        # Оставляем только нужные столбцы
        needed_columns = ['case_id', 'subject', 'group_id', 'last_response_at']
        df = df[needed_columns]

        # Фильтруем DataFrame в зависимости от периода
        if period == 'day':
            filtered_df = df[
                (df['last_response_at'].apply(lambda x: parser.parse(x).timestamp()) >= utc_today_6am) &
                (df['last_response_at'].apply(
                    lambda x: parser.parse(x).timestamp()) <= utc_today_6pm)
            ]
        elif period == 'night':
            filtered_df = df[
                (df['last_response_at'].apply(lambda x: parser.parse(x).timestamp()) >= utc_yesterday_6pm) &
                (df['last_response_at'].apply(
                    lambda x: parser.parse(x).timestamp()) <= utc_today_6am)
            ]
        else:
            raise ValueError(
                "Invalid period. Allowed values are 'day' or 'night'.")

        return filtered_df

    def __get_report_title(self, period):
        # Получаем текущее время в UTC
        utc_now = datetime.now(pytz.UTC)

        # Получаем текущую дату
        today = utc_now.date()

        # Форматирование даты в нужный формат
        date_format = "%d.%m.%Y"

        if period == 'night':
            # До 7 утра UTC
            yesterday = today - timedelta(days=1)
            return f"Отчёт за {yesterday.strftime(date_format)} - {today.strftime(date_format)} (ночь)"
        else:
            # После 7 утра UTC
            return f"Отчёт за {today.strftime(date_format)} (день)"

    def __get_period(self):
        period = ''
        utc_now = datetime.now(pytz.UTC)
        current_hour = utc_now.hour

        if current_hour < 7:
            # До 7 утра UTC
            period = 'night'
        else:
            period = 'day'
        return period

    def __load_schedule(self):
        print("loading sched")

        # Аутентификация с использованием учетных данных сервисного аккаунта
        service_account_info = json.loads(self.CREDS)
        creds = Credentials.from_service_account_info(
            service_account_info, scopes=self.SCOPES)

        # Создание клиента для работы с Google Sheets
        client = gspread.authorize(creds)

        # Открытие таблицы по ID
        sheet_id = os.getenv('MOA_TABLE_ID_SCHEDULE')
        sched = client.open_by_key(sheet_id)

        return sched

    def __get_sheet_num(self):
        # Задаем дату сентября 2023
        start_date = datetime(2023, 9, 1)

        # Получаем текущую дату
        current_date = datetime.now()

        # Вычисляем разницу между датами
        difference = relativedelta(current_date, start_date)

        # Получаем количество прошедших месяцев
        return difference.years * 12 + difference.months

    def __get_schedule(self, sched):
        worksheet = sched.get_worksheet(self.__get_sheet_num())

        # employees = []

        def format_name(full_name):
            # Разделяем имя на части
            parts = full_name.split()

            # Проверяем, что у нас есть как минимум фамилия и имя
            if len(parts) < 2:
                return "Неверный формат имени"

            # Берем фамилию и имя
            last_name, first_name = parts[:2]
            print(last_name, first_name)

            # Преобразуем имя в нужный формат
            formatted_name = f"{first_name.capitalize()} {last_name[0].upper()}."

            return formatted_name

        schedule = dict()
        now = datetime.now()
        this_month_days = calendar.monthrange(now.year, now.month)[1]

        for i in range(3, 12):
            selected = worksheet.row_values(i)
            name = format_name(selected[0])
            # employees.append(name)
            schedule[name] = selected[1:this_month_days]

        # print(employees)
        return schedule

    def __find_employees(self, schedule, day, period):
        day_employees = []
        night_employees = []

        for employee, shifts in schedule.items():
            if period == 'day':
                if day - 1 < len(shifts) and shifts[day - 1] in ['Д', 'ДЧ']:
                    day_employees.append(employee)
                elif day - 1 < len(shifts) and shifts[day - 1] == 'Н':
                    night_employees.append(employee)
            elif period == 'night':
                if day > 1 and day - 2 < len(shifts) and shifts[day - 2] == 'Н':
                    night_employees.append(employee)
                if day - 1 < len(shifts) and shifts[day - 1] in ['Д', 'ДЧ']:
                    day_employees.append(employee)

        return day_employees, night_employees

    def __join_names(self, names):
        if not names:  # Если список пустой
            return ''

        if len(names) == 1:
            return ''.join(names)

        first = ', '.join(names[:len(names) - 1])
        return " и ".join([first, names[-1]])

    def __get_greeting(self, schedule, period):
        day = datetime.now(pytz.UTC).day

        day_employees, night_employees = self.__find_employees(
            schedule, day, period)

        first = None
        second = None
        if period == 'day':
            first = "На смене были: " + self.__join_names(day_employees) + "\n"
            second = self.__join_names(
                night_employees) + " на смене, добрый вечер!"
        else:
            first = "На смене были: " + \
                self.__join_names(night_employees) + "\n"
            second = self.__join_names(
                day_employees) + " на смене, доброе утро!"
        return first + second

    def generate_report12(self):
        period = self.__get_period()
        df = self.__get_data_counts()
        df_processed = self.__get_all_processed(period)
        counts = self.__get_counts(df)
        greet = self.__get_greeting(self.schedule, period)

        text = (f"{self.__get_report_title(period)}\n\n"
                "Входящих звонков - \n"
                "Исходящих звонков - \n\n"

                f"Обработанных тикетов - {len(df_processed)}\n"
                f"В работе тикетов - {counts['Все']}\n\n"

                f"🔴 Визиты - {counts['Визиты']}\n"
                f"🟢 Возвраты - {counts['Возвраты']}\n"
                f"🔵 Разное - {counts['Разное']}\n"
                f"🟠 Кейсы - {counts['Кейсы']}\n\n"

                f"{greet}")

        return text
