"""Configure the logger for the whole project.

To use it, add the following lines at the top of your file:

from biodatasets.utils import logger

log = logger.get(__name__)

The logger can be used in the file by calling :
- log.debug('My debug message')
- log.info('My info message')
- log.warning('My warning message')
- log.critical('My critical message')
- log.exception('My exception message')

The last log can be used in try / except statement and it will log the full traceback

By default the log level is set to INFO, if you want to run as DEBUG you can do the following:
LOGLEVEL=DEBUG python ....
"""
import logging
import os
import sys


def get(name: str) -> logging.Logger:
    """Create a logger for the given module name.

    It is composed of 1 handler:
        - stdout_handler: output the log to sys.stdout

    Args:
        name: module name used to called the 'get' method

    Returns:
        the logger configured with 1 handler
    """
    logger = logging.getLogger(name)
    logger.propagate = False

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())
    stdout_handler.setFormatter(CustomFormatter())

    logger.addHandler(stdout_handler)

    return logger


class CustomFormatter(logging.Formatter):
    r"""Custom formatter to have aligned logs.

    If a \n is used in the log message it will log 2 lines

    Examples :
    2020-04-03 14:20:21 | DEBUG    | logger:log_me:57         | This is an info log
    2020-04-03 14:20:21 | CRITICAL | logger:log_me:57         | This is a critical log
    """

    message_width = 110
    cpath_width = 32
    date_format = "%Y-%m-%d %H:%M:%S"

    def format(self, record) -> str:  # noqa: A003
        """Main method to format a given record.

        Args:
            record: record object to display

        Returns:
            record formatted as string
        """
        cpath = f"{record.module}:{record.funcName}:{record.lineno}"
        cpath = cpath[-self.cpath_width :].ljust(self.cpath_width)

        date = self.formatTime(record, self.date_format)
        prefix = f"{date} | {record.levelname : <8} | PID {record.process: <5} | {cpath}"

        lines = record.getMessage().split("\n")

        # fixing max length
        limited_lines = []
        for line in lines:
            while len(line) > self.message_width:
                splitting_position = self.message_width
                substring = line[: splitting_position - 1]
                last_space_position = substring.rfind(" ")
                if last_space_position > 0:
                    splitting_position = last_space_position

                substring = line[:splitting_position]
                limited_lines.append(substring)
                line = line[splitting_position:]

            limited_lines.append(line)

        formatted_messages = []
        for line in limited_lines:
            formatted_messages.append(f"{prefix} | {line}")

        final_message = "\n".join(formatted_messages).rstrip()

        if record.exc_info and not record.exc_text:
            # Cache the traceback text to avoid converting it multiple times
            record.exc_text = self.formatException(record.exc_info)

        if record.exc_text:
            if final_message[-1:] != "\n":
                final_message += "\n"

            final_message += record.exc_text

        return final_message
