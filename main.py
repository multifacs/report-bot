import telebot
import os
from datetime import datetime

from report7 import Report7
from report12 import Report12

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

def main():
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    bot = telebot.TeleBot(BOT_TOKEN)
    
    # Определение области доступа
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    # Путь к файлу с учетными данными
    CREDS_FILE = "clean-emblem-410317-69ea78d2ab1c.json"
    
    rep7 = Report7(CREDS_FILE, SCOPES)
    rep12 = Report12(CREDS_FILE, SCOPES)

    @bot.message_handler(commands=['report12'])
    def handle_report_command(message):
        # Generate a report (this is just a placeholder for now)
        report = rep12.generate_report12()
        # Send the report back to the user
        # bot.reply_to(message, report)
        bot.send_message(message.chat.id, report)
        
    # Handler for the /report command
    @bot.message_handler(commands=['report7'])
    def handle_report_command(message):
        
        print(message.chat.id)
        
        # Generate a report (this is just a placeholder for now)
        report = rep7.generate_report7()
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

if __name__ == "__main__":
    main()