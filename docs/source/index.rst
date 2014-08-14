.. Caesium documentation master file, created by
   sphinx-quickstart on Wed Aug 13 08:53:28 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Caesium's documentation!
===================================

Contents:

.. toctree::
   :maxdepth: 4

   caesium

Installation
============

To install via pip::

    pip install caesium

Example Application
===================

There is a quickstart project you can clone and install easily: `CaesiumQuickstart <http://github.com/urbn/CaesiumQuickstart>`_

A simple application that utilizes Caesium for comment content::

    import tornado.ioloop
    import tornado.web
    from tornado.options import options
    import tornado.httpserver
    from caesium.handler import BaseRestfulMotorHandler
    from caesium.document import BaseAsyncMotorDocument, AsyncRevisionStackManager
    from settings import settings
    import logging

    class CommentHandler(BaseRestfulMotorHandler):

        def initialize(self):
            self.object_name = "comment"
            self.client = BaseAsyncMotorDocument(self.object_name, self.settings)

    url_patterns = [
        (r"/comment", CommentHandler),
        (r"/comment/([0-9a-zA-Z]+)", CommentHandler),
    ]

    class App(tornado.web.Application):

        def __init__(self):
            """App wrapper constructor, global objects within our Tornado platform should be managed here."""
            self.logger = logging.getLogger(self.__class__.__name__)
            tornado.web.Application.__init__(self, url_patterns, **settings)

            #Document publisher, this allows for patches to be applied
            document_publisher = tornado.ioloop.PeriodicCallback(AsyncRevisionStackManager(settings).publish,
                                                                settings['scheduler']["timeout_in_milliseconds"],
                                                                io_loop=tornado.ioloop.IOLoop.current()
            )

            document_publisher.start()

    application = App()

    if __name__ == "__main__":

        logger = logging.getLogger()
        http_server = tornado.httpserver.HTTPServer(application, xheaders=True)
        http_server.listen(options.port)

        try:
            tornado.ioloop.IOLoop.instance().start()
        except KeyboardInterrupt:
            logger.info("\nStopping server on port %s" % options.port)


Sample settings.py
==================

Here is an example settings.py file to go along with the above app.py::

    import tornado
    import logging, logging.config
    import tornado.template
    from tornado.log import LogFormatter as TornadoLogFormatter
    from tornado.options import define, options
    import os
    import motor

    path = lambda root,*a: os.path.join(root, *a)

    ROOT = os.path.dirname(os.path.abspath(__file__))
    CONF_PATH="%s/%s" % (ROOT, "conf")
    MEDIA_ROOT = path(ROOT, 'apps/media')
    TEMPLATE_ROOT = path(ROOT, 'apps/templates')

    define("port", default=8888, help="run on the given port", type=int)
    define("config", default=None, help="tornado config file")
    define("debug", default=False, help="debug mode")

    if options.config:
        tornado.options.parse_config_file(options.config)

    tornado.options.parse_command_line()

    settings = {}

    #Scheduler settings if you choose to use it
    settings['scheduler']= {
        "timeout_in_milliseconds": 2000,
        "lazy_migrated_published_by_default": True,
        "collections" : ["comment"]
    }

    #Static mongo connection settings
    settings['mongo'] = {}
    settings['mongo']['host'] = "localhost"
    settings['mongo']['port'] = 27017
    settings['mongo']['db'] = "test"

    #Mongo client
    settings['db'] = motor.MotorClient("mongodb://%s:%s" % (settings['mongo']['host'], settings['mongo']['port']))[settings['mongo']['db']]

    settings['debug'] = options.debug
    settings['static_path'] = path(ROOT, 'static/')
    settings['cookie_secret'] = "bbb2b20ab0189b93ba0ae55ac571c214185bea9e"
    settings['xsrf_cookies'] = False
    settings['template_loader'] = tornado.template.Loader(TEMPLATE_ROOT)
    settings['session_cookie'] = 'user'
    settings['annonymous_user'] = "Annonymous"

    LOG_LEVEL = "INFO"

    # See: https://docs.python.org/2/library/logging.html
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "MISL %(asctime)s - %(processName)-10s: %(name)-15s %(levelname)-8s %(message)s",
            },
            'tornado': {
                    '()': TornadoLogFormatter,
                    'fmt': '%(color)s[%(levelname)1.1s %(asctime)s %(name)s.%(funcName)s:%(lineno)d]%(end_color)s %(message)s',
                    'color': True
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": LOG_LEVEL,
                "formatter": "tornado",
                "stream": "ext://sys.stdout"
            }
        },
        "loggers": {
            "transmit": {
                "level": LOG_LEVEL,
                "propagate": False,
                "handlers": ["console"]
            },
        },
        "root": {
            "level": LOG_LEVEL,
            "handlers": ["console"]
        }
    }

    logging.config.dictConfig(LOGGING_CONFIG)




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

