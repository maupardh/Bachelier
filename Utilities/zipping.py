import logging
from io import BytesIO
import zipfile

# SIMPLE TOOLS


def unzip_string_with_zipfile(zipped_string):

    unzipped_string = ''
    logging.info('Unzipping..')
    try:
        zip_file = zipfile.ZipFile(BytesIO(zipped_string))
        for name in zip_file.namelist():
            unzipped_string += zip_file.open(name).read().decode('UTF-8', 'ignore')
        logging.info('Unzipping successful')
    except Exception as err:
        logging.critical('Unzip failed with message: %s' % err)

    return unzipped_string


def unzip_file_to_string_with_zipfile(file_path):

    content = ''
    logging.info('Unzipping file path %s' % file_path)
    try:
        with zipfile.ZipFile(file_path) as zf:
            for filename in zf.namelist():
                with zf.open(filename) as myfile:
                    content += myfile.read().decode('UTF-8', 'ignore')
    except Exception as err:
        logging.critical('Unzipping failed with error message: %s' % err)
    return content


def zip_file_with_zipfile(paths_to_compress, destination_path):

    zf = zipfile.ZipFile(destination_path, mode='w')
    try:
        for p in paths_to_compress:
            zf.write(p)
    except Exception as err:
        logging.critical('Zipping failed to path %s with error message: %s' % (destination_path, err))
    zf.close()


def zip_string_with_zipfile(content, destination_path, file_name='content.csv'):

    compression = zipfile.ZIP_DEFLATED
    zf = zipfile.ZipFile(destination_path, mode='w')
    try:
        zf.writestr(file_name, content, compress_type=compression)
    except Exception as err:
        logging.critical('Zipping failed to path %s with error message: %s' % (destination_path, err))
    zf.close()

