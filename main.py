import telebot
import os
from datetime import datetime

from report7 import Report7
from report12 import Report12
from scheduled import Scheduled
from misc import send_long_message

def main():
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    bot = telebot.TeleBot(BOT_TOKEN)
    
    # Определение области доступа
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    # Путь к файлу с учетными данными
    CREDS_FILE = "clean-emblem-410317-69ea78d2ab1c.json"
    
    rep7 = Report7(CREDS_FILE, SCOPES)
    rep12 = Report12(CREDS_FILE, SCOPES)
    sched = Scheduled(bot, rep7)

    @bot.message_handler(commands=['report12'])
    def handle_report_command(message):
        print(f"Chat id: {message.chat.id}")
        
        # Generate a report (this is just a placeholder for now)
        report = rep12.generate_report12()
        # Send the report back to the user
        # bot.reply_to(message, report)
        bot.send_message(message.chat.id, report)
        
    # Handler for the /report command
    @bot.message_handler(commands=['report7'])
    def handle_report_command(message):
        print(f"Chat id: {message.chat.id}")
        
        # Generate a report (this is just a placeholder for now)
        report = rep7.generate_report7()
        # Send the report back to the user
        m = report['Разное']
        send_long_message(bot, message.chat.id, m)
        m = report['Кейсы']
        send_long_message(bot, message.chat.id, m)

    # Start polling (this keeps the bot running and listening for messages)
    bot.infinity_polling(restart_on_change=False)

if __name__ == "__main__":
    main()