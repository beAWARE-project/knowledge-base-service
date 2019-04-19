from bus_producer import BusProducer
from message_2_kb import Message2KB
from reasoner import Reasoner
import sqlite3
import json
import time
from loggers.time_logger import TimeLogger


class IncomingMessagesHandler:
    def __init__(self, webgenesis_conf):
        self.database = 'messages.sqlite'

        # WebGenesis configuration details
        self.webgenesis_conf = webgenesis_conf

        # Create producer
        self.producer = BusProducer()

    def process_database_messages(self):
        # Query for a message
        message = self.retrieve_a_message()

        # While there are messages
        while message != (None, None):
            # Process this message
            self.process_message(message[0], message[1])

            time.sleep(0.02)

            # Get next message
            message = self.retrieve_a_message()

    @TimeLogger.timer_decorator(tags=["populate_reasoner"])
    def process_message(self, message_id, message_text):
        message_json = None

        try:
            message_json = json.loads(message_text)
        except Exception as e:
            print("Error @ IncomingMessagesHandler.process_message()")
            print(e)
        finally:
            # Delete message after being processed
            self.delete_message(message_id)

        # If message successfully parsed into json and contains a "body" field
        if (message_json is not None) and ('body' in message_json):
            # Insert message to KB if necessary
            Message2KB(self.webgenesis_conf, message_json)

            # Run reasoner if necessary
            Reasoner(self.webgenesis_conf, message_json)

    def retrieve_a_message(self):
        try:
            con = sqlite3.connect(self.database)

            cur = con.cursor()
            cur.execute('SELECT MIN(id), message FROM requests')

            result = cur.fetchone()

            cur.close()

            return result

        except sqlite3.Error as e:
            print("Error %s:" % e.args[0])
            return False

    def delete_message(self, message_id):
        try:
            con = sqlite3.connect(self.database)

            with con:
                cur = con.cursor()
                cur.execute("DELETE FROM requests WHERE id=?", (str(message_id),))

        except sqlite3.Error as e:
            print("Error %s:" % e.args[0])
            return False
