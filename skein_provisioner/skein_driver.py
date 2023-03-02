import threading

from skein import Client
from tornado.ioloop import PeriodicCallback
from traitlets.config import LoggingConfigurable


class SkeinDriverProvider(LoggingConfigurable):
    _driver_client = {}
    _driver_client_supervisor = None
    first_time = True
    rlock = threading.RLock()

    def get_skein_driver_client(self):
        self.log.info("[SkeinDriver] get_skein_driver_client")
        cls = type(self)
        if cls in cls._driver_client:
            return cls._driver_client[cls]
        with cls.rlock:
            if cls in cls._driver_client:
                return cls._driver_client[cls]
            cls._driver_client[cls] = Client()
            # start supervisor
            if cls.first_time:
                cls._driver_client_supervisor = PeriodicCallback(self._supervise, 10000, 0.1)
                cls._driver_client_supervisor.start()
                cls.first_time = False
            return cls._driver_client[cls]

    def _supervise(self):
        cls = type(self)
        self.log.info("[SkeinDriver] _supervise")
        driver_client = self.get_skein_driver_client()
        try:
            driver_client.ping()
        except Exception:
            with cls.rlock:
                try:
                    driver_client.ping()
                except Exception:
                    self.restart_yarn_client()

    def restart_yarn_client(self):
        cls = type(self)
        self.log.info("[SkeinDriver] restart_yarn_client")
        with cls.rlock:
            driver_client = self.get_skein_driver_client()
            driver_client.close()
            cls._driver_client[cls] = None
            cls._driver_client[cls] = Client()
