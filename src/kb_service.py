#! python3
from message_listener import ListenerThread
from incoming_messages_handler import IncomingMessagesHandler
import time
import load_credentials
import clear_KB_v3


class KBService:
    def __init__(self, listen_to_topics, webgenesis_conf):
        self.listen_to_topics = listen_to_topics

        # Initiate a thread to listen to the bus and put incoming messages to the database
        self.listener = ListenerThread(self.listen_to_topics)
        self.listener.setDaemon(True)
        self.listener.start()

        # Create a message handler object
        self.incoming_messages_handler = IncomingMessagesHandler(webgenesis_conf)

        self.running = True

    def run_service(self):
        while self.running:
            # Process all messages in the database
            self.incoming_messages_handler.process_database_messages()

            # Sleep for a while
            time.sleep(0.42)

    def stop_service(self):
        # Stop listener thread
        self.listener.stop()

        # Stop message handling
        self.running = False


if __name__ == "__main__":
    # with open("webgenesis_credentials.json", "r") as f:
    #     webgenesis_configuration = json.load(f)

    webgenesis_configuration = load_credentials.LoadCredentials.load_wg_credentials()

    topics = [
        'TOP021_INCIDENT_REPORT',
        'TOP017_video_analyzed',
        'TOP018_image_analyzed',
        'TOP028_TEXT_ANALYSED',
        'TOP040_TEXT_REPORT_GENERATED',
        'TOP001_SOCIAL_MEDIA_TEXT',
        'TOP003_SOCIAL_MEDIA_REPORT',
        'TOP111_SYSTEM_INITIALIZATION',
        'TOP019_UAV_media_analyzed',
        'TOP112_SUMMARY_TRIGGER',
        'TOP006_INCIDENT_REPORT_CRCL',
        'TOP007_UPDATE_INCIDENT_RISK'
    ]

    # Decorate terminal
    print('\033[95m' + "\n***********************")
    print("*** KB SERVICE v1.0 ***")
    print("***********************\n" + '\033[0m')
    
    print("WebGenesis URL:", webgenesis_configuration["hostname"])

    clear_KB_v3.main()

    kb_service = KBService(listen_to_topics=topics, webgenesis_conf=webgenesis_configuration)
    kb_service.run_service()
