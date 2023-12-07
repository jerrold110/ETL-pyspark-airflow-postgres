import logging

x = 10/0
try:
    10/0
    logging.info('Division has no problem')
except Exception as err:
    logging.debug('This message should go to the log file')
    logging.info('So should this')
    logging.warning('Problem Exception')
    logging.error(str(err))

