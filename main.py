import pandas as pd
import telebot

import requests

import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pytz
from dateutil import parser

import gspread
from google.oauth2.service_account import Credentials

import os

BOT_TOKEN = os.getenv('BOT_TOKEN')
bot = telebot.TeleBot(BOT_TOKEN)
# Определение области доступа
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Путь к файлу с учетными данными
CREDS_FILE = "clean-emblem-410317-69ea78d2ab1c.json"   

group_id = {
    'Корректировки': 82522,
    'Регистрации': 86531,
    'Возвраты': 82521,
    'Разное': 83860,
    'Кейсы': 83508
}

def get_page(params):
    # Basic Authentication
    staff_email = os.getenv('MOA_EMAIL')
    api_key = os.getenv('MOA_API_KEY')

    # URL
    url = 'https://mileonair.omnidesk.ru/api/cases.json'

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

def get_data_counts():
    pages = '1', '2', '3', '4', '5'
    df = None
    all = []
    for page in pages:
        params = {
            'status': 'waiting',
            'page': page
        }
        data = get_page(params)
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
    
def get_counts(df):
    counts = {
        'Визиты': df['group_id'].value_counts()[group_id['Корректировки']] + df['group_id'].value_counts()[group_id['Регистрации']],
        'Возвраты': df['group_id'].value_counts()[group_id['Возвраты']],
        'Разное': df['group_id'].value_counts()[group_id['Разное']],
        'Кейсы': df['group_id'].value_counts()[group_id['Кейсы']]
    }
    counts['Все'] = counts['Визиты'] + counts['Возвраты'] + counts['Разное'] + counts['Кейсы']
    return counts

def get_all_processed(period):
    # Получаем текущее время в UTC
    utc_now = datetime.now(pytz.UTC)
    utc_today_6am = utc_now.replace(hour=6, minute=0, second=0, microsecond=0).timestamp()
    utc_today_6pm = utc_now.replace(hour=18, minute=0, second=0, microsecond=0).timestamp()
    utc_yesterday_6pm = utc_now.replace(day=utc_now.day - 1, hour=18, minute=0, second=0, microsecond=0).timestamp()

    pages = '1', '2', '3', '4', '5'
    df = None
    all = []
    for page in pages:
        params = {
            'page': page,
            'sort': 'response_desc',
        }
        data = get_page(params)
        del data['total_count']
        cases = [data[key]['case'] for key in data.keys()]
        df = pd.DataFrame(cases)
        last = parser.parse(df.loc[99]['last_response_at']).timestamp()
        all.append(df)
        if (period == 'day'):
            if (last < utc_today_6am):
                break
        if (period == 'night'):
            if (last < utc_yesterday_6pm):
                break
        
    df = pd.concat(all, ignore_index=True)
    needed_columns = ['case_id', 'subject', 'group_id', 'last_response_at']
    df = df[needed_columns]
    
    filtered_df = None
    
    if (period == 'day'):
        # Фильтруем DataFrame
        filtered_df = df[
            (df['last_response_at'].apply(lambda x: parser.parse(x).timestamp()) >= utc_today_6am) &
            (df['last_response_at'].apply(lambda x: parser.parse(x).timestamp()) <= utc_today_6pm)
        ]
    if (period == 'night'):
        # Фильтруем DataFrame
        filtered_df = df[
            (df['last_response_at'].apply(lambda x: parser.parse(x).timestamp()) >= utc_yesterday_6pm) &
            (df['last_response_at'].apply(lambda x: parser.parse(x).timestamp()) <= utc_today_6am)
        ]

    return filtered_df

def get_report_title(period):
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
    
def get_period():
    period = ''
    utc_now = datetime.now(pytz.UTC)
    current_hour = utc_now.hour

    if current_hour < 7:
        # До 7 утра UTC
        period = 'night'
    else:
        period = 'day'
    return period

def load_schedule():
    print("loading sched")
    
    # Аутентификация с использованием учетных данных сервисного аккаунта
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    
    # Создание клиента для работы с Google Sheets
    client = gspread.authorize(creds)
    
    # Открытие таблицы по ID
    sheet_id = os.getenv('MOA_TABLE_ID_SCHEDULE')
    sched = client.open_by_key(sheet_id)

    return sched

def get_sheet_num():
    # Задаем дату сентября 2023
    start_date = datetime(2023, 9, 1)

    # Получаем текущую дату
    current_date = datetime.now()

    # Вычисляем разницу между датами
    difference = relativedelta(current_date, start_date)

    # Получаем количество прошедших месяцев
    return difference.years * 12 + difference.months

def get_schedule(sched):
    worksheet = sched.get_worksheet(get_sheet_num())

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
    
def find_employees(schedule, day, period):
    day_employees = []
    night_employees = []

    for employee, shifts in schedule.items():
        if period == 'day':
            if shifts[day-1] in ['Д', 'ДЧ']:
                day_employees.append(employee)
            elif shifts[day-1] == 'Н':
                night_employees.append(employee)
        elif period == 'night':
            if day > 1 and shifts[day-2] == 'Н':
                night_employees.append(employee)
            if shifts[day-1] in ['Д', 'ДЧ']:
                day_employees.append(employee)

    return day_employees, night_employees

def join_names(names):
    if len(names) == 1:
        return ''.join(names)
    
    first = ', '.join(names[:len(names) - 1])
    return " и ".join([first, names[-1]])
    

def get_greeting(schedule, period):
    day = datetime.now(pytz.UTC).day
    
    day_employees, night_employees = find_employees(schedule, day, period)
    
    first = None
    second = None
    if period == 'day':
        first = "На смене были: " + join_names(day_employees) + "\n"
        second = join_names(night_employees) + " на смене, добрый вечер!"
    else:
        first = "На смене были: " + join_names(night_employees) + "\n"
        second = join_names(day_employees) + " на смене, доброе утро!"
    return first + second

sched = load_schedule()
schedule = get_schedule(sched)

def generate_report12():
    period = get_period()
    df = get_data_counts()
    df_processed = get_all_processed(period)
    counts = get_counts(df)
    greet = get_greeting(schedule, period)
    
    text = (f"{get_report_title(period)}\n\n"
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
    
@bot.message_handler(commands=['report12'])
def handle_report_command(message):
    # Generate a report (this is just a placeholder for now)
    report = generate_report12()
    # Send the report back to the user
    # bot.reply_to(message, report)
    bot.send_message(message.chat.id, report)

def load_doc():
    print("loading doc")
    
    # Аутентификация с использованием учетных данных сервисного аккаунта
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    
    # Создание клиента для работы с Google Sheets
    client = gspread.authorize(creds)
    
    # Открытие таблицы по ID
    sheet_id = os.getenv('MOA_TABLE_ID_DAILY')
    doc = client.open_by_key(sheet_id)

    return doc

def generate_misc(doc):
    # Получаем нужный лист по ID
    worksheet = doc.get_worksheet_by_id(19278)

    # Получаем все значения из колонки I, начиная с 3-й строки
    column_i_values = worksheet.col_values(9)[2:]  # 9 - это номер колонки I

    # Преобразуем каждое значение в строку и создаем список
    result = []
    for value in column_i_values:
        arr = str(value).split(" ")
        arr[0] = "<b>" + arr[0] + "</b>"
        result.append(" ".join(arr))
    result_str = "\n".join(result)
    
    return result_str

def generate_cases(doc):
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

def generate_report7():
    doc = load_doc()
    report = {"Разное": f"Разное\n\n{generate_misc(doc)}",
              "Кейсы": f"Кейсы\n\n{generate_cases(doc)}",
              }
    return report

def send_long_message(bot, message, m):
    if len(m) > 4095:
        parts = []
        lines = m.split('\n')
        current_part = ''
        
        for line in lines:
            if len(current_part) + len(line) + 1 <= 4095:  # +1 для \n
                current_part += line + '\n'
            else:
                if current_part:
                    parts.append(current_part)
                current_part = line + '\n'
        
        if current_part:  # Добавляем последнюю часть
            parts.append(current_part)
            
        for part in parts:
            # bot.reply_to(message, text=part, parse_mode="HTML")
            bot.send_message(message.chat.id, text=part, parse_mode="HTML")
    else:
        # bot.reply_to(message, text=m, parse_mode="HTML")
        bot.send_message(message.chat.id, text=m, parse_mode="HTML")

# Handler for the /report command
@bot.message_handler(commands=['report7'])
def handle_report_command(message):
    
    print(message.chat.id)
    
    # Generate a report (this is just a placeholder for now)
    report = generate_report7()
    # Send the report back to the user
    m = report['Разное']
    send_long_message(bot, message, m)
    m = report['Кейсы']
    send_long_message(bot, message, m)
    
from apscheduler.schedulers.background import BackgroundScheduler

def tick():
    print('Tick! The time is: %s' % datetime.now())
    bot.send_message(os.getenv('MOA_CHAT_ID'), 'Tick! The time is: %s' % datetime.now())
    
scheduler = BackgroundScheduler()
scheduler.add_job(tick, 'cron', hour=5, minute=7)
scheduler.start()

# Start polling (this keeps the bot running and listening for messages)
bot.polling()
