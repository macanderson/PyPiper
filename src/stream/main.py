import sys
import logging
import time
from decimal import Decimal

from src.stream.models.schema import AvroSchema, schema, SchemaField
from src.stream.models.app import App

__version_info__ = (1, 10, 0)
__version__ = "%s.%s.%s" % __version_info__

logger = logging.getLogger('Tradesignals-io')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.addHandler(logging.FileHandler('./logs/log.txt'))

__all__ = ['TestOutput', '__version_info__', '__version__']


@schema
class TestOutput(AvroSchema):
    name: str = SchemaField(default='joe blow')()
    age: int = SchemaField(default=30)()
    random: float = SchemaField(default=13223.13934834)()
    dec: Decimal = SchemaField(logical_type='decimal', precision=10, scale=2)()

    class SchemaConfig:
        name = 'TestOutput'
        type = 'record'
        namespace = 'com.tradesignals.io'
        doc = 'Test output schema'



tt = TestOutput(name='my name', age=38, random=123.123, dec=Decimal(123.22))

if __name__ == "__main__":
    output_topic = 'topic_1'
    my_app = App(name=output_topic, writer=TestOutput, producer=None, admin_client=None, schema_registry_client=None)
    print(tt.to_dict())
    while True:
        my_app.producer.produce(
            topic=output_topic, 
            key='1',
            value=tt,
            on_delivery=my_app.delivery_cb
        )
        my_app.producer.poll(0)
        time.sleep(1)
