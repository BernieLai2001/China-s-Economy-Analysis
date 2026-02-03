import schedule
import threading
import time
import logging

logger = logging.getLogger(__name__)

class DownloaderScheduler:
    def __init__(self, downloader):
        self.downloader = downloader
        self.running = False

    def start_daily(self, hour=2, minute=0):
        schedule.clear()
        schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(self.downloader.run)
        self.running = True
        threading.Thread(target=self.loop, daemon=True).start()
        logger.info("Scheduler started (daily)")

    def loop(self):
        while self.running:
            schedule.run_pending()
            time.sleep(1)

    def stop(self):
        self.running = False
        schedule.clear()
        logger.info("Scheduler stopped")
