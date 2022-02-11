import logging
from logging.handlers import RotatingFileHandler


class Handlers:
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s [%(threadName)s] [] [system] ['
                                      'rcms-metrics] [%(name)s] %(message)s')

    def console(self):
        handler = logging.StreamHandler()
        handler.setFormatter(self.formatter)
        handler.setLevel(logging.INFO)
        return handler

    def file(self):
        handler = RotatingFileHandler("logstash.log",
                                      maxBytes=1000000, backupCount=1)
        handler.setLevel(logging.INFO)
        handler.setFormatter(self.formatter)
        return handler


appender = Handlers()
