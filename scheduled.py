from apscheduler.schedulers.background import BackgroundScheduler
import os
from misc import send_long_message

class Scheduled:
    def __init__(self, rep7):
        self.rep7 = rep7
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.__scheduledReport7, 'cron', hour=5, minute=58)
        scheduler.start()
        
    def __scheduledReport7(self):
        report = self.rep7.generate_report7()
        # Send the report back to the user
        m = report['Разное']
        send_long_message(self.bot, os.getenv('MOA_CHAT_ID'), m)
        m = report['Кейсы']
        send_long_message(self.bot, os.getenv('MOA_CHAT_ID'), m)
        