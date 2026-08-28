import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

class ETLLogger:
    loggers = {}

    @classmethod
    def get_log_format(cls):
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        return formatter

    @classmethod
    def add_console_handler(cls,logger: logging.Logger, formatter):
        """
        Add console handler into logger
        """
        consoleHandler = logging.StreamHandler(sys.stdout)
        consoleHandler.setFormatter(formatter)
        logger.addHandler(consoleHandler)

    @classmethod
    def add_file_handler(cls,logger: logging.Logger,log_dir, log_file, formatter):
        """
        Add file handler for server and local machine
        """
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        full_file_path = log_path / log_file


        fileHandler = RotatingFileHandler(full_file_path,
                                          mode='a',
                                          maxBytes=5*1024*1024, # đặt max dung lượng của file log = 5 MB
                                          backupCount=3, # Tối đa 3 file backup được giữ lại
                                          encoding='utf-8')
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)

    @classmethod
    def get_logger(cls, name = "F1_ETL",level = logging.INFO , log_to_file = True,log_dir="logs", log_file = "etl.log"):
        """ 
        Generate a logger. 
        :param name: Name of the module/pipeline to record in the log. 
        :param log_to_file: Whether to write logs to a file. 
        :param log_dir: Directory containing the log file. 
        :param log_file: Name of the log file. 
        """

        #singleton
        if name in cls.loggers:
            return cls.loggers[name]
        
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = False

        if not logger.handlers:
            formatter = cls.get_log_format()
            #Adding console handler
            cls.add_console_handler(logger, formatter)

            #Checking and adding file handler
            if log_to_file:
                cls.add_file_handler(logger,log_dir, log_file, formatter)
        cls.loggers[name] = logger
        return logger
