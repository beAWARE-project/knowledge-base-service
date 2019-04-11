import threading
from bus_consumer import BusConsumer


class ListenerThread(threading.Thread):
    def __init__(self, topics):
        threading.Thread.__init__(self)

        self.topics = topics
        self.consumer = None

    def run(self):
        print("Initiated listener on topics", self.topics)

        # Create consumer object
        self.consumer = BusConsumer()

        # Start listening to bus
        self.consumer.listen(self.topics)

    def stop(self):
        self.consumer.stop()

    def get_topics(self):
        return self.topics

