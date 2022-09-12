import logging
import sys
import boto3
from io import StringIO
import os

class LogFormatter(logging.Formatter):

    COLOR_CODES = {
        logging.CRITICAL: "\033[1;35m", # bright/bold magenta
        logging.ERROR:    "\033[1;31m", # bright/bold red
        logging.WARNING:  "\033[1;33m", # bright/bold yellow
        logging.INFO:     "\033[0;37m", # white / light gray
        logging.DEBUG:    "\033[1;30m"  # bright/bold black / dark gray
    }

    RESET_CODE = "\033[0m"

    def __init__(self, color, *args, **kwargs):
        super(LogFormatter, self).__init__(*args, **kwargs)
        self.color = color

    def format(self, record, *args, **kwargs):
        if (self.color == True and record.levelno in self.COLOR_CODES):
            record.color_on  = self.COLOR_CODES[record.levelno]
            record.color_off = self.RESET_CODE
        else:
            record.color_on  = ""
            record.color_off = ""
        return super(LogFormatter, self).format(record, *args, **kwargs)


class logger():
    
    def __init__(self, script_name, console_log_output="stdout", console_log_level="info", console_log_color=True, \
            logfile_log_level="debug", logfile_log_color=True ):
        self.script_name = os.path.splitext(os.path.basename(script_name))[0]
        self.console_log_output = console_log_output
        self.console_log_level = console_log_level
        self.console_log_color = console_log_color
        self.logfile_log_level = logfile_log_level
        self.logfile_log_color = logfile_log_color
        self.s3_log = self._setup_logging()
        
    # Setup logging
    def _setup_logging(self):
    # Create logger
    # For simplicity, we use the root logger, i.e. call 'logging.getLogger()'
    # without name argument. This way we can simply use module methods for
    # for logging throughout the script. An alternative would be exporting
    # the logger, i.e. 'global logger; logger = logging.getLogger("<name>")'
        logger = logging.getLogger()
        log_line_template="%(color_on)s[%(created)d] [%(threadName)s] [%(levelname)-8s] %(message)s%(color_off)s"

        # Set global log level to 'debug' (required for handler levels to work)
        logger.setLevel(logging.DEBUG)

        # Create console handler
        console_log_output = self.console_log_output.lower()
        if (console_log_output == "stdout"):
            console_log_output = sys.stdout
        elif (console_log_output == "stderr"):
            console_log_output = sys.stderr
        else:
            print("Failed to set console output: invalid output: '%s'" % console_log_output)
            return False
        console_handler = logging.StreamHandler(console_log_output)

        # Set console log level
        try:
            console_handler.setLevel(self.console_log_level.upper()) # only accepts uppercase level names
        except:
            print("Failed to set console log level: invalid level: '%s'" % self.console_log_level)
            return False

        # Create and set formatter, add console handler to logger
        console_formatter = LogFormatter(fmt=log_line_template, color=self.console_log_color)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # Create log file handler
        try:
            logfile_handler = logging.FileHandler(f'{self.script_name}.log')
        except Exception as exception:
            print("Failed to set up log file: %s" % str(exception))
            return False

        # Set log file log level
        try:
            logfile_handler.setLevel(self.logfile_log_level.upper()) # only accepts uppercase level names
        except:
            print("Failed to set log file log level: invalid level: '%s'" % self.logfile_log_level)
            return False

        # Create and set formatter, add log file handler to logger
        logfile_formatter = LogFormatter(fmt=log_line_template, color=self.logfile_log_color)
        logfile_handler.setFormatter(logfile_formatter)
        logger.addHandler(logfile_handler)

        # add s3 logging functionality
        s3_log = StringIO()
        s3_log_handler = logging.StreamHandler(s3_log)
        # Set log file log level
        try:
            s3_log_handler.setLevel(self.logfile_log_level.upper()) # only accepts uppercase level names
        except:
            print("Failed to set log file log level: invalid level: '%s'" % self.logfile_log_level)
            return False

        # Create and set formatter, add log file handler to logger
        logfile_formatter = LogFormatter(fmt=log_line_template, color=self.logfile_log_color)
        s3_log_handler.setFormatter(logfile_formatter)
        logger.addHandler(s3_log_handler)

        # boto3 put_object operations result in the entire file's content getting logged as a hex-encoded byte string
        # following line suppreses this behavior
        logging.getLogger("boto3.resources.action").setLevel(logging.INFO)
        # we want to suppres [WARNING ] Connection pool is full, discarding connection, hence we suppres urrllib < ERROR
        logging.getLogger("urllib3").setLevel(logging.ERROR)

        # Success
        return s3_log


class s3Logger():

    def __init__(self, logger, s3_bucket, s3_log_key):
        self.logger = logger.s3_log
        self.s3_bucket = s3_bucket
        self.s3_log_key = s3_log_key

    def send_logs(self):
        s3 = boto3.client("s3")
        s3.put_object(Body=self.logger.getvalue(), Bucket=self.s3_bucket, Key=self.s3_log_key)  


    

    






