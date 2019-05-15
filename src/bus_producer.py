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

        # Check if topic exists and create it if not
        if not self.handle_topic(topic):
            return False

        # Produce and flush message to bus
        try:
            self.producer.produce(topic, message.encode('utf-8'), 'key', -1, self.on_delivery)
            self.producer.flush()
        except Exception as err:
            print('Sending data failed')
            print(err)
            return False

        return True

    def handle_topic(self, topic_name):

        # Create rest client to handle topics
        try:
            rest_client = rest.MessageHubRest(self.credentials['kafka_admin_url'], self.credentials['api_key'])
        except Exception as e:
            print(e)
            return False

        # List all topics
        try:
            response = rest_client.list_topics()
            topics = json.loads(response.text)
        except Exception as e:
            print(e)
            return False

        # Check if desired topic exists in topic list
        topic_exists = False

        for topic in topics:
            if topic['name'] == topic_name:
                topic_exists = True

        # If topic does not exist
        if not topic_exists:

            # Create topic
            try:
                response = rest_client.create_topic(topic_name, 1, 1)
                print(response.text)
            except Exception as e:
                print(e)
                return False

        return True

    def on_delivery(self, err, msg):
        if err:
            print(err)
            # We could retry sending the message

