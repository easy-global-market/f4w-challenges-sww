import requests
import pickle
import pandas as pd
from datetime import datetime
import pytz
import json


def ngsi_entity(dma, plantId, observedAt, measure, period):
    ngsi_json = {
        '@context': AT_CONTEXT,
        'id': plantId,
        'type': 'WaterConsumption',
        'dma': {
            'type': 'Property',
            'value': dma
        },
        'litres': {
            'type': 'Property',
            'value': measure,
            'observedAt': observedAt,
            'unitCode': 'LTR',
            'period': {
                'type': 'Property',
                'value': period,
                'unitCode': 'SEC'
            }
        }
    }
    return (ngsi_json)


def ngsi_patch(observedAt, measure, period):
    ngsi_json = {
        '@context': AT_CONTEXT,
        "litres": {
            'type': 'Property',
            'value': measure,
            'observedAt': observedAt,
            'unitCode': 'LTR',
            'period': {
                'type': 'Property',
                'value': period,
                'unitCode': 'SEC'
            }
        }
    }
    return (ngsi_json)


if __name__ == '__main__':
    '''
    Main: Load file and upload content to Context Broker
    '''
    # Reading some configuration
    with open('./configuration.json', 'r') as f:
        configuration = json.load(f)

    # NGSI-LD Context
    AT_CONTEXT = configuration['AT_CONTEXT']
    # File(s) to upload
    CSV_FILES = configuration['FILES']
    # URL for entities
    URL_ENTITIES = configuration['URL_ENTITIES']

    headers_post = {
        'Content-Type': 'application/ld+json'
    }

    timezone_UTC = pytz.timezone('UTC')

    # Read each file and upload their content into Context Broker
    for csv_file in CSV_FILES:

        # Read content of the file
        data = pd.read_csv(csv_file)

        # Group the file by DMA. First record of a DMA will be
        # uploaded as a CREATE, then other records will be uploaded
        # as a PATCH
        grouped = data.groupby('DMA')
        for dma, group in grouped:

            # For each record of a DMA we use the plantId, period
            # the date/time of observation and the measure
            # period is hard-coded as 900 second, i.e. 15mn as
            # found in the file
            created = False
            for index, row in group.iterrows():
                plantId = 'urn:ngsi-ld:WaterConsumption:' + dma
                period = 900
                observedAt = row['DTT']
                observedAt = datetime.strptime(observedAt, '%d/%m/%Y %H:%M')
                observedAt = timezone_UTC.localize(observedAt).isoformat()
                measure = row['Litres']
                if created:
                    # PATCH
                    print('patching ...')
                    entity = ngsi_patch(observedAt, measure, period)
                    URL_PATCH = URL_ENTITIES+plantId+'/attrs'
                    r = requests.post(URL_PATCH, json=entity, headers=headers_post)
                    print(f'Status: [{r.status_code}]')
                else:
                    # CREATE
                    print('creating ...')
                    entity = ngsi_entity(dma, plantId, observedAt,
                                         measure, period)
                    r = requests.post(URL_ENTITIES, json=entity,
                                      headers=headers_post)
                    print(f'Status: [{r.status_code}]')
                    created = True
