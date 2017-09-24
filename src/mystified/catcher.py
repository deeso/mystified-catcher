import logging
import SocketServer
import sys
import json
import os

from tzlocal import get_localzone
from kombu import Connection
import Queue


class CatcherService(SocketServer.BaseRequestHandler):
    NAME = 'Catcher'
    KOMBU_URI = "redis://127.0.0.1:6379"
    KOMBU_Q = "mystified-catcher"
    SYSLOG_MSG_TYPE = {
        0: "EMERGENCY",
        1: "ALERT",
        2: "CRITICAL",
        3: "ERROR",
        4: "WARNING",
        5: "NOTICE",
        6: "INFORMATIONAL",
        7: "DEBUG",
    }
    MY_TZ = os.environ.get('CATCHER_TZ', 'NOT_SET')

    @classmethod
    def setup_logging(cls, log_level=logging.DEBUG):
        fmt_str = '[%(asctime)s] %(name)s - %(levelname)s - %(message)s'
        logging.getLogger().setLevel(log_level)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter(fmt_str)
        ch.setFormatter(formatter)
        logging.getLogger().addHandler(ch)

    @classmethod
    def set_kombu(cls, uri, queue):
        cls.KOMBU_URI = uri
        logging.debug("Setting kombu uri to: %s" % uri)
        if cls.KOMBU_URI is None:
            raise Exception("Kombu broker uri must be set")
        cls.KOMBU_Q = queue
        logging.debug("Setting kombu queue to: %s" % queue)

    @classmethod
    def set_name(cls, name):
        cls.NAME = name
        logging.debug("Setting svc name to: %s" % name)

    @classmethod
    def get_server(cls, shost, sport):
        try:
            logging.debug("Starting the syslog listener")
            server = SocketServer.UDPServer((shost, sport), cls)
            return server
        except:
            raise

    @classmethod
    def get_tz(cls):
        # return str(get_localzone())
        return cls.MY_TZ

    def handle(self):
        logging.debug("Handling syslog message")
        data = bytes.decode(self.request[0].strip())
        # logging.debug("Resolving syslog message source")
        host = self.client_address[0]
        json_msg = {'syslog_msg': data,
                    'syslog_host': host,
                    'catcher_name': self.NAME,
                    'catcher_tz': self.get_tz()}
        if self.KOMBU_URI is None:
            return

        logging.debug("Putting message in queue")
        with Connection(self.KOMBU_URI) as conn:
            q = conn.SimpleQueue(self.KOMBU_Q)
            q.put(json.dumps(json_msg))
            q.close()
        logging.debug("Message handling done")
