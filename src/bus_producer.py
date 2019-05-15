from confluent_kafka import Producer
import rest
import json


class BusProducer:
    def __init__(self):

        # Pre-shared credentials
        self.credentials = json.load(open('bus_credentials.json'))

        # Construct required configuration
        self.configuration = {
            'client.id': 'KB_producer',
            'group.id': 'KB_producer_group',
            'bootstrap.servers': ','.join(self.credentials['kafka_brokers_sasl']),
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': '/etc/ssl/certs',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.credentials['api_key'][0:16],
            'sasl.password': self.credentials['api_key'][16:48],
            'api.version.request': True
        }

        self.producer = Producer(self.configuration)

    def send(self, topic, message):

        # Produce and flush message to bus
        try:
            self.producer.produce(topic, message.encode('utf-8'), 'key', -1, self.on_delivery)
            self.producer.flush()
        except Exception as err:
            print('Sending data failed')
            print(err)
            return False

        return True

    def on_delivery(self, err, msg):
        if err:
            print(err)
            # We could retry sending the message

