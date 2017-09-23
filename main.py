from mystified.catcher import CatcherService as Catcher
import logging
import argparse


parser = argparse.ArgumentParser(description='A syslog catcher service.')

parser.add_argument('-shost', type=str, default='',
                    help='syslog service listener address')
parser.add_argument('-sport', type=int, default=5000,
                    help='syslog listener port')

parser.add_argument('-broker_uri', type=str, default=None,
                    help='kombu queue address')
parser.add_argument('-broker_queue', type=str, default='default',
                    help='kombu queue name to publish to')

parser.add_argument('-name', type=str, default=Catcher.NAME,
                    help='kombu queue address')

V = 'log levels: INFO: %d, DEBUG: %d, WARNING: %d' % (logging.INFO,
                                                      logging.DEBUG,
                                                      logging.WARNING)
parser.add_argument('-log_level', type=int, default=logging.DEBUG, help=V)

if __name__ == "__main__":
    args = parser.parse_args()
    if args.broker is None:
        raise Exception("Must specify the uri for kombu")
    try:
        Catcher.setup_logging(args.log_level)
        Catcher.set_kombu(args.broker_uri, args.broker_queue)
        Catcher.set_name(args.name)
        server = Catcher.get_server(args.shost, args.sport)
        logging.debug("Starting the syslog catcher")
        server.serve_forever(poll_interval=0.5)
    except (IOError, SystemExit):
        raise
    except KeyboardInterrupt:
        raise
