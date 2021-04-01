from config.settings import AT_CONTEXT

__author__ = 'Fernando López'


class Payload:
    def __init__(self):
        pass

    def create(self, dma, period, observedAt, measure):
        entity_id = "urn:ngsi-ld:WaterConsumption:" + dma

        data = {
            "@context": AT_CONTEXT,
            "id": entity_id,
            "type": "WaterConsumption",
            "dma": {
                "type": "Property",
                "value": dma
            },
            "litres": {
                "type": "Property",
                "value": measure,
                "observedAt": observedAt.astype(str),
                "unitCode": "LTR",
                "period": {
                    "type": "Property",
                    "value": int(period),
                    "unitCode": "SEC"
                }
            }
        }

        return entity_id, data

    def patch(self, observedAt, measure, period):
        data = {
            "@context": AT_CONTEXT,
            "litres": {
                "type": "Property",
                "value": measure,
                "observedAt": observedAt.astype(str),
                "unitCode": "LTR",
                "period": {
                    "type": "Property",
                    "value": period,
                    "unitCode": "SEC"
                }
            }
        }

        return data
