import tkinter as tk
from tkinter import ttk, scrolledtext
import threading
import logging

from csrc_downloader import CSRCDownloader
from scheduler import DownloaderScheduler

class TextHandler(logging.Handler):
    def __init__(self, widget):
        super().__init__()
        self.widget = widget

    def emit(self, record):
        msg = self.format(record)
        self.widget.after(0, lambda: self.widget.insert(tk.END, msg + "\n"))

class App:
    def __init__(self, root):
        self.root = root
        root.title("CSRC 自动下载器")
        root.geometry("900x600")

        frame = ttk.Frame(root)
        frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(frame, text="栏目ID").pack(side="left")
        self.section = tk.StringVar(value="c100122")
        ttk.Entry(frame, textvariable=self.section, width=20).pack(side="left", padx=5)

        ttk.Button(frame, text="立即下载", command=self.start).pack(side="left", padx=5)
        ttk.Button(frame, text="每天更新", command=self.daily).pack(side="left", padx=5)

        self.log = scrolledtext.ScrolledText(root)
        self.log.pack(fill="both", expand=True)

        handler = TextHandler(self.log)
        handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        logging.getLogger().addHandler(handler)

        self.downloader = None
        self.scheduler = None

    def start(self):
        def run():
            url = f"http://www.csrc.gov.cn/csrc/{self.section.get()}/common_list.shtml"
            self.downloader = CSRCDownloader(url)
            self.downloader.run()
        threading.Thread(target=run, daemon=True).start()

    def daily(self):
        if not self.downloader:
            url = f"http://www.csrc.gov.cn/csrc/{self.section.get()}/common_list.shtml"
            self.downloader = CSRCDownloader(url)
        self.scheduler = DownloaderScheduler(self.downloader)
        self.scheduler.start_daily()

if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()
