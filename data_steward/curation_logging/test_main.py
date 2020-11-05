import logging
from curation_logging.slack_logging_handler import initialize_slack_logging

if __name__ == "__main__":
    initialize_slack_logging()
    logging.info('Do not send this')
    logging.debug('Do not send this')
    logging.warning('Slack message sent by logging handler!')
