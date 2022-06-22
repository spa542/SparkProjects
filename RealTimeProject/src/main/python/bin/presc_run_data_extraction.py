import logging
import logging.config


### Load the configuration file
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def extract_files(df, format, file_path, split_no, header_req, compression_type):
    try:
        logger.info('Extraction - extract_files() is started...')
        df.coalesce(split_no).write.format(format).save(file_path, header=header_req, compression=compression_type)
    except Exception as e:
        logger.error('Error is the method - extract_files(). Please check the stack trace. ' + str(e), exc_info=True)
        raise
    else:
        logger.info('Extraction - extract_files() is complete.\n\n')

