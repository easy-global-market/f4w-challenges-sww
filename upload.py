from config.settings import CSV_FILES, LOGLEVEL
from processor.ngsi import NGSI

__author__ = 'Fernando López'


if __name__ == '__main__':
    '''
    Main: Load file(s) and upload content to Context Broker
    '''
    ngsi = NGSI(loglevel=LOGLEVEL)

    # Read each file and upload their content into Context Broker
    [ngsi.process(file=csv_file) for csv_file in CSV_FILES]
