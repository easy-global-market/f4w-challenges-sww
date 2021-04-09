from config.settings import AT_CONTEXT, URL_BROKER, PROPERTIES, SCOPE
from config.logging_conf import LoggingConf
from pytz import timezone
from datetime import datetime
from pandas import read_csv, to_datetime
from requests import post
from logging import error, info, debug
from time import sleep
from processor.payload import Payload

__author__ = 'Fernando López'


class NGSI(LoggingConf):
    def __init__(self, loglevel):
        super(NGSI, self).__init__(loglevel=loglevel, log_file='f4w-challenge-sww.log')

        self.entity_id = 0

        self.headersPost = {
            'Content-Type': 'application/ld+json'
        }

        self.url_entities = URL_BROKER + '/ngsi-ld/v1/entities'

        self.payload = Payload()

        self.timezone_UTC = timezone('UTC')

    def process(self, file):
        info(f"Starting process of file: {file[0]}")

        # Read content of the file
        df = read_csv(filepath_or_buffer=file[1], sep=',')

        # Extract only the columns 'DMA', 'DTT', and 'Litres'
        df = df[['DMA', 'Period', 'DTT', 'Litres']]

        # Create a column with Date + Time and transform to datetime: 01/04/2020 00:00
        df['DTT'] = to_datetime(df['DTT'], format='%d/%m/%Y %H:%M', dayfirst=True, utc=True)

        # Group the file by DMA. First record of a DMA will be
        # uploaded as a CREATE, then other records will be uploaded
        # as a PATCH
        df = df.groupby('DMA')

        [self.process_group(group) for dma, group in df]

    def process_group(self, group):
        # Sort the data by DTT
        group = group.sort_values(by=['DTT'])

        # First record of a measure will be uploaded as a CREATE,
        # then other records will be uploaded as a PATCH
        row_1 = group[:1]
        self.create(dma=row_1.DMA.values[0],
                    period=row_1.Period.values[0],
                    date_observed=row_1.DTT.values[0],
                    measure=row_1.Litres.values[0])

        # Get the last values of the excel file: PATCH
        last = group.tail(len(group.index) - 1)

        # Iterating over two columns, use `zip`
        try:
            [self.update(date_observed=row.DTT.to_datetime64(), measure=row.Litres, period=row.Period)
             for row in last.itertuples()]
        except ValueError as e:
            error("There was a problem parsing the csv data")

    def create(self, dma, period, date_observed, measure):
        self.entity_id, data = self.payload.create(dma=dma, period=period, observedAt=date_observed, measure=measure)

        debug(f"Create: Data to be uploaded:\n {data}\n")

        # CREATE
        info('Creating ...')

        try:
            r = post(self.url_entities, json=data, headers=self.headersPost)
        except Exception as e:
            error(e)

        info(f'Create Status: [{r.status_code}]')

        # Wait some time to proceed with the following
        sleep(SCOPE)

    def update(self, date_observed, measure, period):
        data = self.payload.patch(observedAt=date_observed, measure=measure, period=period)

        debug(f"Update: Data to be uploaded:\n {data}\n")

        # UPDATE
        info('Updating ...')
        url_patch = self.url_entities + '/' + self.entity_id + '/attrs'
        r = post(url_patch, json=data, headers=self.headersPost)
        info(f'Update Status: [{r.status_code}]')

        # Wait some seconds to proceed with the following request
        sleep(SCOPE)
