import json
from bus_producer import BusProducer
from datetime import datetime, timedelta
from math import radians, sin, cos, acos
from random import randint, choice
import string
from loggers.time_logger import TimeLogger
from Utilities import get_evac_status
from wg_connection import wg_client


class PersistentFields:
    __descriptions = dict()
    __titles = dict()

    @staticmethod
    def get_description(psap_id):
        if psap_id in PersistentFields.__descriptions:
            return PersistentFields.__descriptions[psap_id]
        return None

    @staticmethod
    def get_title(psap_id):
        if psap_id in PersistentFields.__titles:
            return PersistentFields.__titles[psap_id]
        return None

    @staticmethod
    def set_description(psap_id, description):
        PersistentFields.__descriptions[psap_id] = description

    @staticmethod
    def set_title(psap_id, title):
        PersistentFields.__titles[psap_id] = title

    @staticmethod
    def clear_all():
        PersistentFields.__descriptions = dict()
        PersistentFields.__titles = dict()

    @staticmethod
    def apply_persistent(outgoing_dictionary):
        try:
            psap_id = outgoing_dictionary['body']['incidentID']
            if "title" in outgoing_dictionary["body"] and outgoing_dictionary["body"]['title'].strip() != "":
                PersistentFields.set_title(psap_id, title=outgoing_dictionary["body"]['title'])
                print("setting new title:" + str(outgoing_dictionary["body"]['title']))
            elif PersistentFields.get_title(psap_id) is not None:
                outgoing_dictionary["body"]['title'] = PersistentFields.get_title(psap_id)
                print("Using existing title:" + str(PersistentFields.get_title(psap_id)))

            if "description" in outgoing_dictionary["body"] and outgoing_dictionary["body"][
                'description'].strip() != "":
                PersistentFields.set_description(psap_id, description=outgoing_dictionary["body"]['description'])
                print("setting new description:" + str(outgoing_dictionary["body"]['description']))
            elif PersistentFields.get_description(psap_id) is not None:
                outgoing_dictionary["body"]['description'] = PersistentFields.get_description(psap_id)
                print("Using existing description:" + str(PersistentFields.get_description(psap_id)))
        except:
            print("warn: could not apply persistant")
            pass

    @staticmethod
    def update_fields_from_incoming(psap_id, incoming_message, outgoing_message):
        try:
            title = None
            if "title" in incoming_message["body"]:
                title = incoming_message["body"]["title"]
            elif PersistentFields.get_title(psap_id) is not None:
                title = PersistentFields.get_title(psap_id)

            if title is not None:
                if "report" in title.lower():
                    title = "Report"
                elif "incident" in title.lower():
                    tokens = title.split("_")
                    title = "_".join(tokens[:2])
                outgoing_message["body"]["title"] = title

            description = None
            if "description" in incoming_message["body"]:
                description = incoming_message["body"]["description"]
            elif PersistentFields.get_description(psap_id) is not None:
                description = PersistentFields.get_description(psap_id)

            if description is not None:
                outgoing_message["body"]["description"] = description
        except:
            print("problem with incoming message in persistant fields")
            pass

    @staticmethod
    def _generate_title_description(psap_id, title, description):
        """
        If title is None, return the locally saved title (or None if there isn't one)
        If description is None, return the locally saved description(or None if there isn't one)

        If title has value, save it locally, and return it.
        If description has value, save it locally, and return it.


        :param psap_id:
        :param title:
        :param description:
        :return:
        """
        if description is None:
            description = PersistentFields.get_description(psap_id)
        else:
            PersistentFields.set_description(psap_id, description)
        if title is None:
            title = PersistentFields.get_title(psap_id)
        else:
            PersistentFields.set_title(psap_id, title)

        return title, description


persistent_fields = PersistentFields()


class Reasoner:
    @TimeLogger.timer_decorator(tags=["reasoner"])
    def __init__(self, webgenesis_conf, message_json=None):
        self.conf = webgenesis_conf
        self.incoming_message = message_json

        self.__default_cluster_radius = 200

        self.__default_drone_cluster_radius = self.__default_cluster_radius

        # self.webgenesis_client = WebGenesisClient(self.conf)
        self.webgenesis_client = wg_client

        if self.incoming_message is not None:
            self.topic = self.incoming_message['header']['topicName']

            # Run the corresponding topic function
            try:
                #     if self.webgenesis_client.login() != 200:
                #         print("login response is different than 200")
                #         raise Exception

                getattr(self, self.topic.lower())()

                # self.webgenesis_client.logout()

            except:
                pass

    @TimeLogger.timer_decorator(tags=["reasoner_021"])
    def top021_incident_report(self):
        """
        if alert -> sends 101
        if update and not(type=="video"/"image"/"") -> send 101
        """

        # Copy incoming message
        outgoing_message = self.incoming_message

        # Change topic name in header
        outgoing_message['header']['topicName'] = "TOP101_INCIDENT_REPORT"

        # Handle PSAP incident ID
        incident_id = outgoing_message['body']['incidentID']

        # If an alert, calculate the PSAP incident ID
        if outgoing_message['header']['actionType'] == "Alert":
            lat = outgoing_message['body']['position']['latitude']
            long = outgoing_message['body']['position']['longitude']

            # Check if this incident is nearby previous incidents
            psap_incident_id = self.calculate_psap_incident_id(incident_id, lat, long)

            # Insert the PSAP incident ID to the KB
            self.webgenesis_client.set_incident_report_psap_id(incident_id, psap_incident_id)
            # Set the PSAP incident ID to the TOP101 message
            outgoing_message['body']['incidentID'] = psap_incident_id

        # If an update, get the existing PSAP incident ID
        else:
            psap_incident_id = self.webgenesis_client.get_incident_report_psap_id(incident_id)
            outgoing_message['body']['incidentID'] = psap_incident_id

            # TODO: TEMP - If attachement type is empty, do nothing
            try:
                if outgoing_message['body']['attachments'][0]["attachmentType"] == "":
                    return
            except:
                pass

        # Get the location (lat,long) of the psap incident
        psap_indicent_location = self.webgenesis_client.get_location_of_incident_report(
            outgoing_message['body']['incidentID'])
        if psap_indicent_location is not None:
            outgoing_message['body']['position']['latitude'] = psap_indicent_location["lat"]
            outgoing_message['body']['position']['longitude'] = psap_indicent_location["long"]

        # Update incident category
        outgoing_message['body']['incidentCategory'] = self.webgenesis_client.get_incident_category(
            outgoing_message['body']['incidentID'])

        # TODO: TEMP
        # Update incident originator
        outgoing_message['body']['incidentOriginator'] = "KB"

        # Update severity
        outgoing_message['body']['severity'] = self.webgenesis_client.get_incident_cluster_severity(
            outgoing_message['body']['incidentID'])

        if Reasoner._proceed_with_TOP101(outgoing_message):
            # Produce outgoing message
            persistent_fields.apply_persistent(outgoing_message)
            self.produce_message(outgoing_message['header']['topicName'], outgoing_message)
            print(">> TOP101 Incident report sent to PSAP")
        else:
            print("TOP101 is of type image or video, no TOP101 will be sent")

    @TimeLogger.timer_decorator(tags=["top805"])
    def top805_kbs_triggers(self):

        try:
            operation = self.incoming_message["body"]["title"]
            if operation == "clear_kbs":
                print(">> Clearing persistent title and description fields...")
                persistent_fields.clear_all()
                print(">> Removing report texts from SQLite DB")
                wg_client.delete_all_incident_report_texts_in_sqlite()

            elif operation == "clear_persistent":
                print(">> Clearing persistent title and description fields")
                persistent_fields.clear_all()

            elif operation == "clear_sqlite":
                print(">> Removing report texts from SQLite DB")
                wg_client.delete_all_incident_report_texts_in_sqlite()

        except Exception as e:
            print("Could not perform TOP805 operation")

    @staticmethod
    def _proceed_with_TOP101(message):
        """when there are attachements in the msg and they have attachmentType == video/image then do not send 101
        or if incidentType =="https://www.iosb.fraunhofer.de/ilt/mobileApp#CompletedTask"
        """
        try:
            attachment_type = message['body']['attachments'][0]["attachmentType"]
            if message['body']["incidentType"] == "https://www.iosb.fraunhofer.de/ilt/mobileApp#CompletedTask":
                return True
            if attachment_type == "image" or attachment_type == "video":
                return False
            else:
                return True
        except:
            return True

    @TimeLogger.timer_decorator(tags=["reasoner_018"])
    def top018_image_analyzed(self):
        try:
            incident_id = self.incoming_message['body']['incidentID']
            results_file_url = self.incoming_message['body']['im_analysis']
            analyzed_file_url = self.incoming_message['body']['im_analyzed']
            media_timestamp = self.incoming_message['body']['media_timestamp']
        except Exception as e:
            print("Error 1 @ Reasoner.top018_image_analyzed")
            print("Error in message:\n", e)
            return

        psap_id = self.webgenesis_client.get_incident_report_psap_id(incident_id)

        # Perform reasoning
        self.run_reasoning()

        # Send message to PSAP
        # Get previous incident report text
        try:
            outgoing_message = json.loads(self.webgenesis_client.get_incident_report_text_from_sqlite(incident_id))
            # print("outgoing message from sqlite: " + str(outgoing_message))
        except Exception as e:
            print("Error 2 @ Reasoner.top018_image_analyzed")
            print("Error retrieving previous incident report text:\n", e)
            return

        # Change topic name in header
        outgoing_message['header']['topicName'] = "TOP101_INCIDENT_REPORT"

        # Change reference
        outgoing_message['header']['references'] = results_file_url

        # Get raw attachment name
        try:
            raw_attachment_name = outgoing_message["body"]["attachments"][0]["attachmentName"]
        except Exception as e:
            print("Error 3 @ Reasoner.top018_image_analyzed")
            print(e)
            raw_attachment_name = "Image from undefined source"

        # Add analyzed image attachment
        analyzed_attachment = {
            "attachmentTimeStampUTC": media_timestamp,
            "attachmentType": "image",
            "attachmentName": raw_attachment_name + " - Analyzed",
            "attachmentURL": analyzed_file_url
        }
        outgoing_message['body']['attachments'].append(analyzed_attachment)

        # Find psap id
        psap_id = self.webgenesis_client.get_incident_report_psap_id(incident_id)

        # Add priority and severity
        incident_uri = self.webgenesis_client.get_incident_uri(incident_id)
        outgoing_message['body']['priority'] = self.webgenesis_client.get_incident_priority(incident_uri)
        outgoing_message['body']['severity'] = self.webgenesis_client.get_incident_cluster_severity(psap_id)

        # print("Outgoing message with severity and priority:"+str(outgoing_message))

        # Change incident id to PSAP incident id
        outgoing_message['body']['incidentID'] = psap_id

        # Get the location (lat,long) of the psap incident
        psap_indicent_location = self.webgenesis_client.get_location_of_incident_report(
            outgoing_message['body']['incidentID'])
        if psap_indicent_location is not None:
            outgoing_message['body']['position']['latitude'] = psap_indicent_location["lat"]
            outgoing_message['body']['position']['longitude'] = psap_indicent_location["long"]

        # Update incident category
        outgoing_message['body']['incidentCategory'] = self.webgenesis_client.get_incident_category(
            outgoing_message['body']['incidentID'])

        # TODO: TEMP
        # Update incident originator
        outgoing_message['body']['incidentOriginator'] = "KB"

        persistent_fields.update_fields_from_incoming(psap_id=psap_id, incoming_message=self.incoming_message,
                                                      outgoing_message=outgoing_message)

        persistent_fields.apply_persistent(outgoing_dictionary=outgoing_message)

        # Produce outgoing message
        self.produce_message(outgoing_message['header']['topicName'], outgoing_message)

        # Update incident report text in KB
        self.webgenesis_client.update_incident_report_text_in_sqlite(incident_id,
                                                                     json.dumps(outgoing_message, indent=3))
        print(">> TOP101 Incident report with severity/priority sent to PSAP")

        # Check if a place of relief capacity is affected by this analysis
        self.update_place_of_relief_capacity(incident_id)
        print(">> Checking if image analysis affects a place of relief capacity")

        # Request report from report generator
        # Request is asked using the PSAP id, since it will contain all cluster's data
        self.request_report_from_generator(
            incident_id=incident_id,
            psap_id=outgoing_message['body']['incidentID'],
            language=outgoing_message['body']['language'],
            priority=outgoing_message['body']['priority'],
            severity=outgoing_message['body']['severity']
        )

        print(">> Report generation was requested from MRG")

    @TimeLogger.timer_decorator(tags=["reasoner_017"])
    def top017_video_analyzed(self):
        try:
            incident_id = self.incoming_message['body']['incidentID']
            results_file_url = self.incoming_message['body']['vid_analysis']
            analyzed_file_url = self.incoming_message['body']['vid_analyzed']
            media_timestamp = self.incoming_message['body']['media_timestamp']
        except Exception as e:
            print("Error 1 @ Reasoner.top017_video_analyzed")
            print("Error in message:\n", e)
            return

        # Find psap id
        psap_id = self.webgenesis_client.get_incident_report_psap_id(incident_id)

        # Perform reasoning
        self.run_reasoning()

        # Send message to PSAP
        # Get previous incident report text
        try:
            outgoing_message = json.loads(self.webgenesis_client.get_incident_report_text_from_sqlite(incident_id))
        except Exception as e:
            print("Error 2 @ Reasoner.top017_video_analyzed")
            print("Error retrieving previous incident report text:\n", e)
            return

        # Change topic name in header
        outgoing_message['header']['topicName'] = "TOP101_INCIDENT_REPORT"

        # Change reference
        outgoing_message['header']['references'] = results_file_url

        # Get raw attachment name
        try:
            raw_attachment_name = outgoing_message["body"]["attachments"][0]["attachmentName"]
        except Exception as e:
            print("Error 3 @ Reasoner.top017_video_analyzed")
            print(e)
            raw_attachment_name = "Video from undefined source"

        # Add analyzed image attachment
        analyzed_attachment = {
            "attachmentTimeStampUTC": media_timestamp,
            "attachmentType": "video",
            "attachmentName": raw_attachment_name + " - Analyzed",
            "attachmentURL": analyzed_file_url
        }
        outgoing_message['body']['attachments'].append(analyzed_attachment)

        # Add priority and severity
        incident_uri = self.webgenesis_client.get_incident_uri(incident_id)
        outgoing_message['body']['priority'] = self.webgenesis_client.get_incident_priority(incident_uri)
        outgoing_message['body']['severity'] = self.webgenesis_client.get_incident_cluster_severity(psap_id)

        # Change incident id to PSAP incident id
        outgoing_message['body']['incidentID'] = psap_id

        # Get the location (lat,long) of the psap incident
        psap_indicent_location = self.webgenesis_client.get_location_of_incident_report(
            outgoing_message['body']['incidentID'])
        if psap_indicent_location is not None:
            outgoing_message['body']['position']['latitude'] = psap_indicent_location["lat"]
            outgoing_message['body']['position']['longitude'] = psap_indicent_location["long"]

        # Update incident category
        outgoing_message['body']['incidentCategory'] = self.webgenesis_client.get_incident_category(
            outgoing_message['body']['incidentID'])

        # TODO: TEMP
        # Update incident originator
        outgoing_message['body']['incidentOriginator'] = "KB"

        persistent_fields.update_fields_from_incoming(psap_id=psap_id, incoming_message=self.incoming_message,
                                                      outgoing_message=outgoing_message)

        persistent_fields.apply_persistent(outgoing_dictionary=outgoing_message)

        # Produce outgoing message
        self.produce_message(outgoing_message['header']['topicName'], outgoing_message)

        # Update incident report text
        self.webgenesis_client.update_incident_report_text_in_sqlite(incident_id,
                                                                     json.dumps(outgoing_message, indent=3))

        print(">> TOP101 Incident report with severity/priority sent to PSAP")

        # Check if a place of relief capacity is affected by this analysis
        self.update_place_of_relief_capacity(incident_id)
        print(">> Checking if video analysis affects a place of relief capacity")

        # Request report from report generator
        # Request is asked using the PSAP id, since it will contain all cluster's data
        self.request_report_from_generator(
            incident_id=incident_id,
            psap_id=outgoing_message['body']['incidentID'],
            language=outgoing_message['body']['language'],
            priority=outgoing_message['body']['priority'],
            severity=outgoing_message['body']['severity']
        )

        print(">> Report generation was requested from MRG")

    @TimeLogger.timer_decorator(tags=["reasoner_028"])
    def top028_text_analysed(self):
        try:
            incident_id = self.incoming_message['body']['incidentID']
            language = self.incoming_message['body']['language']
            analysis_results = self.incoming_message['body']['data']
        except Exception as e:
            print("Error 1 @ Reasoner.top028_text_analysed")
            print("Error in message:\n", e)
            return

        # If no findings in text analysis (data field is empty), return
        if not analysis_results:
            return

        # Find psap id
        psap_id = self.webgenesis_client.get_incident_report_psap_id(incident_id)

        # Perform reasoning
        self.run_reasoning()

        # Request report from report generator
        # Get the location of incident report or of incident within the report
        report_location = self.webgenesis_client.get_location_of_incident_report(incident_id)

        # If the incident report does not have a location (i.e. it is a tweet with undefined coordinates and no incident locations)
        if report_location['lat'] == "undefined" and report_location['long'] == "undefined":
            print("No incident location, in TOP028")
            return

        if self.incoming_message['body']['incidentOriginator'] != "SMA":
            # Request is asked using the PSAP id, since it will contain all cluster's data
            self.request_report_from_generator(
                incident_id=incident_id,
                psap_id=self.webgenesis_client.get_incident_report_psap_id(incident_id),
                language=language
            )
            print(">> Report generation was requested from MRG")

    @TimeLogger.timer_decorator(tags=["reasoner_040"])
    def top040_text_report_generated(self):
        try:
            incident_id = self.incoming_message['body']['incidentID']
            description = self.incoming_message['body']['description']
            title = self.incoming_message['body']['title']
        except Exception as e:
            print("Error 1 @ Reasoner.top040_text_report_generated")
            print("Error in message TOP040:\n", e)
            return

        # Send message to PSAP
        # Get previous incident report text
        try:
            outgoing_message = json.loads(self.webgenesis_client.get_incident_report_text_from_sqlite(incident_id))
        except Exception as e:
            print("Error 2 @ Reasoner.top040_text_report_generated")
            print("Error retrieving previous incident report text:\n", e)
            return

        # Change topic name in header
        outgoing_message['header']['topicName'] = "TOP101_INCIDENT_REPORT"

        # Find psap id
        psap_id = self.webgenesis_client.get_incident_report_psap_id(incident_id)

        # title, description = persistant_fields.generate_title_description(psap_id, title=title, description=description)

        # Change title and description
        # outgoing_message['body']['title'] = title
        # outgoing_message['body']['description'] = description

        # Change incident id to PSAP incident id
        outgoing_message['body']['incidentID'] = psap_id

        # Update incident category
        outgoing_message['body']['incidentCategory'] = self.webgenesis_client.get_incident_category(
            outgoing_message['body']['incidentID'])

        # Update severity
        outgoing_message['body']['severity'] = self.webgenesis_client.get_incident_cluster_severity(psap_id)

        # TODO: TEMP
        # Update incident originator
        outgoing_message['body']['incidentOriginator'] = "KB"

        # Get the location (lat,long) of the psap incident
        psap_indicent_location = self.webgenesis_client.get_location_of_incident_report(
            outgoing_message['body']['incidentID'])
        if psap_indicent_location is not None:
            outgoing_message['body']['position']['latitude'] = psap_indicent_location["lat"]
            outgoing_message['body']['position']['longitude'] = psap_indicent_location["long"]

        persistent_fields.update_fields_from_incoming(psap_id=psap_id, incoming_message=self.incoming_message,
                                                      outgoing_message=outgoing_message)

        persistent_fields.apply_persistent(outgoing_dictionary=outgoing_message)

        # Produce outgoing message
        self.produce_message(outgoing_message['header']['topicName'], outgoing_message)

        print(">> TOP101 Incident report with title/description sent to PSAP")

        # Update incident report text
        self.webgenesis_client.update_incident_report_text_in_sqlite(incident_id,
                                                                     json.dumps(outgoing_message, indent=3))

    @TimeLogger.timer_decorator(tags=["reasoner_001"])
    def top001_social_media_text(self):

        # Handle PSAP incident ID
        incident_id = self.incoming_message['body']['incidentID']

        # Check if a position was given
        if 'position' in self.incoming_message['body']:
            # Get lat and long
            lat = self.incoming_message['body']['position']['latitude']
            long = self.incoming_message['body']['position']['longitude']

            # Calculate PSAP incident ID
            psap_incident_id = self.calculate_psap_incident_id(incident_id, lat, long)

        # If no position given, set the incident ID as PSAP incident ID
        else:
            psap_incident_id = incident_id

        # Insert the PSAP incident ID to the KB
        self.webgenesis_client.set_incident_report_psap_id(incident_id, psap_incident_id)

    @TimeLogger.timer_decorator(tags=["reasoner_003"])
    def top003_social_media_report(self):
        # Copy incoming message
        outgoing_message = self.incoming_message
        default_language = "en-US"

        # Change topic name in header
        outgoing_message['header']['topicName'] = "TOP101_INCIDENT_REPORT"

        # Handle PSAP incident ID
        incident_id = outgoing_message['body']['incidentID']

        # Calculate the PSAP incident ID
        lat = outgoing_message['body']['position']['latitude']
        long = outgoing_message['body']['position']['longitude']

        if "language" in self.incoming_message["body"]:
            language = self.incoming_message["body"]["language"]
        else:
            language = default_language

        self.webgenesis_client.update_incident_report_text_in_sqlite(incident_id,
                                                                     json.dumps(self.incoming_message, indent=2))

        # Check if this incident is nearby previous incidents
        psap_incident_id = self.calculate_psap_incident_id(incident_id, lat, long)

        # Insert the PSAP incident ID to the KB
        self.webgenesis_client.set_incident_report_psap_id(incident_id, psap_incident_id)

        # Set the PSAP incident ID to the TOP101 message
        outgoing_message['body']['incidentID'] = psap_incident_id

        # Get the location (lat,long) of the psap incident
        psap_indicent_location = self.webgenesis_client.get_location_of_incident_report(
            outgoing_message['body']['incidentID'])
        if psap_indicent_location is not None:
            outgoing_message['body']['position']['latitude'] = psap_indicent_location["lat"]
            outgoing_message['body']['position']['longitude'] = psap_indicent_location["long"]

        # Get the severity of the cluster
        outgoing_message['body']['severity'] = self.webgenesis_client.get_incident_cluster_severity(psap_incident_id)

        # Update incident category
        outgoing_message['body']['incidentCategory'] = self.webgenesis_client.get_incident_category(
            outgoing_message['body']['incidentID'])

        # TODO: TEMP
        # Update incident originator
        outgoing_message['body']['incidentOriginator'] = "KB"

        persistent_fields.update_fields_from_incoming(psap_id=psap_incident_id, incoming_message=self.incoming_message,
                                                      outgoing_message=outgoing_message)

        persistent_fields.apply_persistent(outgoing_dictionary=outgoing_message)

        # Produce outgoing message
        self.produce_message(outgoing_message['header']['topicName'], outgoing_message)

        print(">> TOP101 Incident report sent to PSAP")

        # Request is asked using the PSAP id, since it will contain all cluster's data
        self.request_report_from_generator(
            incident_id=incident_id,
            psap_id=self.webgenesis_client.get_incident_report_psap_id(incident_id),
            language=language,
            trigger="SMA"
        )
        print(">> Report generation was requested from MRG")

    @TimeLogger.timer_decorator(tags=["reasoner_019"])
    def top019_uav_media_analyzed(self):
        default_language = "en-US"
        try:
            incident_id = self.incoming_message["body"]["incidentID"]
            incident_detected = self.incoming_message["body"]["incident_detected"]
            lat = self.incoming_message['body']['location']['latitude']
            long = self.incoming_message['body']['location']['longitude']
            analyzed_file_url = self.incoming_message['body']['media_analyzed']
            media_timestamp = self.incoming_message["body"]["media_timestamp"]
            if "language" in self.incoming_message["body"]:
                language = self.incoming_message["body"]["language"]
            else:
                language = default_language
        except Exception as e:
            print("Error 1 @ Reasoner.top019_uav_media_analyzed")
            print("Error in message:\n", e)
            return

        evacuation = get_evac_status(self.incoming_message)
        print("The evacuation status: " + str(evacuation))

        # if not incident_detected and evacuation == 'end':
        #     self.request_report_for_evac_end(incident_id,)
        #     print(">> Report generation was requested from MRG")
        #     return

        if not incident_detected and evacuation != 'end':
            return

        # Find psap id
        psap_id = self.webgenesis_client.get_incident_report_psap_id(incident_id)

        # Perform reasoning
        self.run_reasoning()

        # Copy incoming message
        outgoing_message = json.loads(json.dumps(self.incoming_message))

        # Change topic name in header
        outgoing_message['header']['topicName'] = "TOP101_INCIDENT_REPORT"

        # Arrange "body" as required in TOP101
        outgoing_message["body"] = {
            "incidentOriginator": "KB",
            "position": {
                "latitude": lat,
                "longitude": long
            },
            "language": language,
            "startTimeUTC": media_timestamp,
            "title": "UAV footage report from " + media_timestamp,
            "attachments": [
                {
                    "attachmentName": "UAV footage - Analyzed",
                    "attachmentType": "video",
                    "attachmentTimeStampUTC": media_timestamp,
                    "attachmentURL": analyzed_file_url
                }
            ]
        }

        # Handle PSAP incident ID

        # Calculate the PSAP incident ID
        # Check if this incident is nearby previous incidents
        psap_incident_id = self.calculate_psap_incident_id(incident_id, lat, long,
                                                           cluster_radius=self.__default_drone_cluster_radius)

        # Insert the PSAP incident ID to the KB
        self.webgenesis_client.set_incident_report_psap_id(incident_id, psap_incident_id)

        # Set the PSAP incident ID to the TOP101 message
        outgoing_message['body']['incidentID'] = psap_incident_id

        # Get the location (lat,long) of the psap incident
        psap_indicent_location = self.webgenesis_client.get_location_of_incident_report(
            outgoing_message['body']['incidentID'])
        if psap_indicent_location is not None:
            outgoing_message['body']['position']['latitude'] = psap_indicent_location["lat"]
            outgoing_message['body']['position']['longitude'] = psap_indicent_location["long"]

        # Add priority and severity
        incident_uri = self.webgenesis_client.get_incident_uri(incident_id)
        outgoing_message['body']['priority'] = self.webgenesis_client.get_incident_priority(incident_uri)
        outgoing_message['body']['severity'] = self.webgenesis_client.get_incident_cluster_severity(psap_incident_id)

        # Update incident category
        outgoing_message['body']['incidentCategory'] = self.webgenesis_client.get_incident_category(
            outgoing_message['body']['incidentID'])

        persistent_fields.update_fields_from_incoming(psap_id=psap_incident_id, incoming_message=self.incoming_message,
                                                      outgoing_message=outgoing_message)

        persistent_fields.apply_persistent(outgoing_dictionary=outgoing_message)

        # Produce outgoing message TOP101
        self.produce_message(outgoing_message['header']['topicName'], outgoing_message)

        # Update incident report text
        self.webgenesis_client.update_incident_report_text_in_sqlite(incident_id,
                                                                     json.dumps(outgoing_message, indent=3))

        print(">> TOP101 Incident report with severity/priority sent to PSAP")

        # Request report from report generator
        # Request is asked using the PSAP id, since it will contain all cluster's data
        self.request_report_from_generator(
            incident_id=incident_id,
            psap_id=outgoing_message['body']['incidentID'],
            language=language,
            priority=outgoing_message['body']['priority'],
            severity=outgoing_message['body']['severity'],
            evacuation=evacuation
        )

        print(">> Report generation was requested from MRG")

    @TimeLogger.timer_decorator(tags=["reasoner_111"])
    def top111_system_initialization(self):
        print(">> TOP111 system initialization received")

        # Try to parse message
        try:
            center_latitude = self.incoming_message['body']['position']['latitude']
            center_longitude = self.incoming_message['body']['position']['longitude']
            radius = self.incoming_message['body']['district_radius']

        except Exception as e:
            print("Error 1 @ Reasoner.top111_system_initialization()")
            print(e)
            return

        # Handle relief places
        nearby_relief_places = self.get_nearby_relief_places(center_latitude, center_longitude, radius)

        # For each place of relief within the radius
        for place in nearby_relief_places:
            incident_id = str(randint(1, 999999999))

            # Insert place of relief as incident report to the KB
            self.create_place_of_relief_incident_report(relief_place_uri=place['relief_place'], incident_id=incident_id,
                                                        name=place["display_name"])

            # Assemble outgoing message
            outgoing_message = self.create_place_of_relief_message(incident_id, place["display_name"], place["lat"],
                                                                   place["long"])

            # Produce outgoing message
            self.produce_message(outgoing_message['header']['topicName'], outgoing_message)
            persistent_fields.apply_persistent(outgoing_dictionary=outgoing_message)
            print(">> TOP101 Incident report for place of relief " + place["display_name"] + " was sent to PSAP")

    @TimeLogger.timer_decorator(tags=["reasoner_112"])
    def top112_summary_trigger(self):
        print(">> TOP112 summary trigger received")

        # Try to parse message
        try:
            language = self.incoming_message['body']['language']
        except Exception as e:
            print("Error 1 @ Reasoner.top112_summary_trigger()")
            print(e)
            return

        self.request_wrap_up_summary_from_generator(language=language)

    @TimeLogger.timer_decorator(tags=["reasoner_006"])
    def top006_incident_report_crcl(self):

        # Copy incoming message
        outgoing_message = self.incoming_message

        # Change topic name in header
        outgoing_message['header']['topicName'] = "TOP101_INCIDENT_REPORT"

        # Handle PSAP incident ID
        incident_id = outgoing_message['body']['incidentID']

        # Calculate the PSAP incident ID
        lat = outgoing_message['body']['position']['latitude']
        long = outgoing_message['body']['position']['longitude']

        # Check if this incident is nearby previous incidents
        psap_incident_id = self.calculate_psap_incident_id(incident_id, lat, long)

        # Insert the PSAP incident ID to the KB
        self.webgenesis_client.set_incident_report_psap_id(incident_id, psap_incident_id)

        # Set the PSAP incident ID to the TOP101 message
        outgoing_message['body']['incidentID'] = psap_incident_id

        # Update severity
        outgoing_message['body']['severity'] = self.webgenesis_client.get_incident_cluster_severity(
            outgoing_message['body']['incidentID'])

        # If no useful severity value is found, do not inform the PSAP with TOP101
        if outgoing_message['body']['severity'] == "unknown":
            return

        # Get the location (lat,long) of the psap incident
        psap_indicent_location = self.webgenesis_client.get_location_of_incident_report(
            outgoing_message['body']['incidentID'])
        if psap_indicent_location is not None:
            outgoing_message['body']['position']['latitude'] = psap_indicent_location["lat"]
            outgoing_message['body']['position']['longitude'] = psap_indicent_location["long"]

        # Update incident category
        outgoing_message['body']['incidentCategory'] = self.webgenesis_client.get_incident_category(
            outgoing_message['body']['incidentID'])

        # TODO: TEMP
        # Update incident originator
        outgoing_message['body']['incidentOriginator'] = "KB"

        persistent_fields.update_fields_from_incoming(psap_id=psap_incident_id, incoming_message=self.incoming_message,
                                                      outgoing_message=outgoing_message)

        persistent_fields.apply_persistent(outgoing_dictionary=outgoing_message)

        # Produce outgoing message
        self.produce_message(outgoing_message['header']['topicName'], outgoing_message)

        print(">> TOP101 Incident report sent to PSAP")

    @TimeLogger.timer_decorator(tags=["reasoner_007"])
    def top007_update_incident_risk(self):
        description = None
        title = None
        language = "en-US"
        try:
            incident_id = self.incoming_message['body']['incidentID']
            psap_id = self.webgenesis_client.get_incident_report_psap_id(incident_id)
            incident_timestamp = self.incoming_message['body']['incidentDateTimeUTC']
            if "language" in self.incoming_message['body']:
                language = self.incoming_message['body']['language']
            position = self.webgenesis_client.get_location_of_incident_report(psap_id)
            latitude = position["lat"]
            longitude = position["long"]
            if "description" in self.incoming_message['body']:
                description = self.incoming_message['body']['description']
            if "title" in self.incoming_message['body']:
                title = self.incoming_message['body']['title']

        except Exception as e:
            print("Error 1 @ Reasoner.top007_update_incident_risk")
            print("Error in message:\n", e)
            return

        outgoing_message = {
            "header": {
                "topicName": "TOP101_INCIDENT_REPORT",
                "topicMajorVersion": 0,
                "code": 0,
                "note": "",
                "district": "",
                "topicMinorVersion": 3,
                "actionType": "Update",
                "status": "Actual",
                "references": "",
                "scope": "Restricted",
                "specificSender": "",
                "recipients": "",
            },
            "body": {
                "severity": self.webgenesis_client.get_incident_cluster_severity(psap_id),
                "incidentCategory": self.webgenesis_client.get_incident_category(psap_id),
                "incidentOriginator": "KB",
                "position": {
                    "latitude": latitude,
                    "longitude": longitude
                },
                "language": language,
                "incidentID": psap_id,
                "startTimeUTC": incident_timestamp,
            }
        }
        if description is not None:
            outgoing_message['body']['description'] = description
        if title is not None:
            outgoing_message['body']['title'] = title

        persistent_fields.apply_persistent(outgoing_dictionary=outgoing_message)

        # Produce outgoing message
        self.produce_message(outgoing_message['header']['topicName'], outgoing_message)

        print(">> TOP101 Incident report sent to PSAP")

    @TimeLogger.timer_decorator(tags=["reasoner_801"])
    def top801_incident_validation(self):
        try:
            incident_id = self.incoming_message['body']['incidentID']
            spam_flag = self.incoming_message['body']['spam']
        except Exception as e:
            print("Error 1 @ Reasoner.top801_incident_validation")
            print("Error in message TOP040:\n", e)
            return

        if not isinstance(spam_flag, bool):
            return

        ## Send message to PSAP
        # Get previous incident report text
        try:
            # outgoing_message = json.loads(self.webgenesis_client.get_incident_report_text(incident_id))
            outgoing_message = json.loads(self.webgenesis_client.get_incident_report_text_from_sqlite(incident_id))
        except Exception as e:
            print("Error 2 @ Reasoner.top801_incident_validation")
            print("Error retrieving previous incident report text:\n", e)
            return

        # Change topic name in header
        outgoing_message['header']['topicName'] = "TOP101_INCIDENT_REPORT"
        outgoing_message['body']['spam'] = spam_flag

        # Find psap id
        psap_id = self.webgenesis_client.get_incident_report_psap_id(incident_id)

        # Change incident id to PSAP incident id
        outgoing_message['body']['incidentID'] = psap_id

        # Get the location (lat,long) of the psap incident
        psap_indicent_location = self.webgenesis_client.get_location_of_incident_report(
            outgoing_message['body']['incidentID'])
        if psap_indicent_location is not None:
            outgoing_message['body']['position']['latitude'] = psap_indicent_location["lat"]
            outgoing_message['body']['position']['longitude'] = psap_indicent_location["long"]

        # try:  # test
        #     del outgoing_message['body']['description']
        #     print("outgoing with no description"+ outgoing_message)
        # except:
        #     pass

        if spam_flag:  # the message is spam
            description = "INCIDENT WAS FOUND TO BE FAKE! "
            if persistent_fields.get_description(psap_id) is not None:
                description += persistent_fields.get_description(psap_id)

            outgoing_message['body']['description'] = description
            persistent_fields.set_description(psap_id, description)

            if persistent_fields.get_title(psap_id) is not None:
                outgoing_message['body']['title'] = persistent_fields.get_title(psap_id)

            persistent_fields.update_fields_from_incoming(psap_id=psap_id, incoming_message=self.incoming_message,
                                                          outgoing_message=outgoing_message)

            persistent_fields.apply_persistent(outgoing_message)

            # Produce outgoing message
            self.produce_message(outgoing_message['header']['topicName'], outgoing_message)

            print(">> TOP101 Incident report with spam flag was sent to PSAP")

    @TimeLogger.timer_decorator(tags=["request_report_030"])
    def request_report_from_generator(self, incident_id, psap_id, language, priority="undefined", severity="undefined",
                                      evacuation=None, position=None, trigger=None):
        outgoing_message = {
            "header": {
                "status": "Actual",
                "note": "",
                "specificSender": "",
                "district": "Valencia",
                "topicMinorVersion": 0,
                "scope": "Restricted",
                "recipients": "",
                "actionType": "Update",
                "code": 0,
                "topicName": "TOP030_REPORT_REQUESTED",
                "msgIdentifier": "kbs_msg_id_" + str(randint(1, 999999999)),
                "references": "",
                "topicMajorVersion": 1
            },
            "body": {
                "incidentID": incident_id,
                "language": language,
                # "position": self.webgenesis_client.get_location_of_incident_report(psap_id),
                "priority": priority,
                "severity": severity
            }
        }

        if evacuation == 'end' or evacuation == "inProgress":
            outgoing_message['body']['evacuation'] = evacuation

        if position is None:
            outgoing_message['body']['position'] = self.webgenesis_client.get_location_of_incident_report(psap_id)
        else:
            # TODO: typecheck
            outgoing_message['body']['position'] = position

        # Get all incidents that belong to this psap id
        incidents_details_list = self.webgenesis_client.get_incidents_details_from_psap_incident_cluster(psap_id)
        # print("returned incidents : "+str(incidents_details_list))
        if not incidents_details_list:
            print("No incidents found by SPARQL in request report generator ")
            return

        outgoing_message['body']['incidents'] = []
        for incident in incidents_details_list:
            # print("INCIDENT ID: " + str(incident_id) + " REPORT ID : " + str(incident["report_id"]) + " TRIGGER: " + str(trigger))
            if incident_id == incident["report_id"] or trigger == "SMA":
                outgoing_message['body']['incidents'].append(
                    self.create_incident_section_for_report_generator(incident))

        # Produce outgoing message to MRG
        self.produce_message(outgoing_message["header"]["topicName"], outgoing_message)

    def request_wrap_up_summary_from_generator(self, language):
        outgoing_message = {
            "header": {
                "status": "Actual",
                "note": "",
                "specificSender": "",
                "district": "Valencia",
                "topicMinorVersion": 0,
                "scope": "Restricted",
                "recipients": "",
                "actionType": "Update",
                "code": 0,
                "topicName": "TOP033_SUMMARY_REQUESTED",
                "msgIdentifier": "kbs_msg_id_" + str(randint(1, 999999999)),
                "references": "",
                "topicMajorVersion": 1
            },
            "body": {
                "language": language
            }
        }

        # Get all incidents that belong to this psap id
        incidents_details_list = self.webgenesis_client.get_incidents_details_from_all_psap_incident_clusters()

        if not incidents_details_list:
            print("No incidents found by SPARQL")
            return

        outgoing_message['body']['incidents'] = []
        for incident in incidents_details_list:
            outgoing_message['body']['incidents'].append(self.create_incident_section_for_report_generator(incident))

        try:
            import time
            with open("033_" + str(time.time()) + ".json", 'w') as f:
                f.write(json.dumps(outgoing_message, indent=2))
        except:
            print("warn: could not write 033 to file")
            pass

        # Produce outgoing message to MRG
        self.produce_message(outgoing_message["header"]["topicName"], outgoing_message)

    def create_incident_section_for_report_generator(self, incident):
        # print(">> creating section for incident of report " + incident["report_id"])

        section = {}

        # Get details of incident to appropriate fields
        section["mediaType"] = incident["attachment_type"]
        section["timestamp"] = incident["timestamp"]
        section["severity"] = incident["severity"]
        section["priority"] = incident["priority"]
        section["reportId"] = incident["report_id"]
        section["refs"] = incident["refs"]

        if "cluster_id" in incident:
            section["clusterId"] = incident["cluster_id"]

        # Get type of incident
        section["incidentType"] = self.webgenesis_client.get_type_of_incident(incident["incident_uri"])

        # Get participants
        section["participants"] = []
        involved_participants = self.webgenesis_client.get_involved_participants_of_incident(incident["incident_uri"])

        # print("INCIDENT::::::::::" + str(incident))
        for participant in involved_participants:
            # print("involved participant: " + str(participant))
            # print("section participants:"+str(section["participants"]))

            type_exists_in_json = False
            for participant_in_json in section["participants"]:
                if participant["type"] == participant_in_json["type"] and participant["type"] != "Vulnerable Object":
                    participant_in_json["detections"].append({
                        "confidence": participant["confidence"],
                        "risk": participant["risk"]
                    })

                    type_exists_in_json = True

            if not type_exists_in_json:
                section["participants"].append({
                    "type": participant["type"],
                    "role": participant["role"],
                    "label": participant["label"],
                    "number": participant["number"],
                    "refs": participant["refs"],
                    "detections": [{
                        "confidence": participant["confidence"],
                        "risk": participant["risk"]
                    }]
                })

        return section

    def produce_message(self, topic, message, sender="KB"):

        # Set sent time in header
        message['header']['sentUTC'] = self.utc_now()

        # Set sender in header
        message['header']['sender'] = sender

        # Set random message identifier in header
        message['header']['msgIdentifier'] = "kb_msg_identifier_" + ''.join(
            choice(string.ascii_uppercase + string.digits) for _ in range(20))

        # Set district if not set (TEMP)
        if message['header']['district'] == "":
            message['header']['district'] = "Valencia"

        # Check if expiration time is set and hard code it
        if "expirationTimeUTC" in message["body"]:
            message["body"]["expirationTimeUTC"] = self.utc_plus_5_hours()

        # Create producer
        producer = BusProducer()

        # Produce message
        producer.send(topic, json.dumps(message, indent=3))

    def utc_now(self):
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    def utc_plus_5_hours(self):
        return (datetime.utcnow() + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def calculate_psap_incident_id(self, report_id, lat, long, cluster_radius=None):
        print("reportID" + str(report_id))
        print("location" + str(lat) + " " + str(long))

        if cluster_radius is None:
            cluster_radius = self.__default_cluster_radius

        if lat == "undefined" or long == "undefined":
            # TODO: Check if an incident within this incident report has a location to use
            return report_id

        psap_incident_id = None
        min_distance_from_existing_incident = 100000000

        if "_evacuation" in report_id:
            print("is evacuation:" + str(report_id))
            return report_id

        # Get all existing reports that have a psap incident id, with their locations (lat, long)
        existing_incidents = self.webgenesis_client.get_psap_incident_locations()
        # print("existing Incidents psap ids:" + str([i["psap_id"] for i in existing_incidents]))
        # exclude clusters that are from evacuation, do not put new incidents in evacuation cluster
        existing_incidents = [inc for inc in existing_incidents if "_evacuation" not in inc['psap_id']]
        # print("existing Incidents without evacuation: " + str([i["psap_id"] for i in existing_incidents]))
        # For each existing incident
        for existing_incident in existing_incidents:

            if existing_incident['lat'] != "undefined" and existing_incident['long'] != "undefined":

                # Calculate the distance from the given location
                distance = self.calculate_distance_between_locations(lat, long, existing_incident['lat'],
                                                                     existing_incident['long'])

                if distance < min_distance_from_existing_incident:
                    psap_incident_id = existing_incident['psap_id']
                    min_distance_from_existing_incident = distance

        # If a previous psap incident was found closer than 50m
        if min_distance_from_existing_incident <= cluster_radius:
            print("psap incident id:" + str(psap_incident_id))
            return psap_incident_id

        # Else, the incident should not be grouped with any previous psap incident and create a new group
        else:
            return report_id

    def calculate_distance_between_locations(self, lat1, long1, lat2, long2):

        start_lat = radians(float(lat1))
        start_lon = radians(float(long1))
        end_lat = radians(float(lat2))
        end_lon = radians(float(long2))

        distance = 6371.01 * acos(
            sin(start_lat) * sin(end_lat) + cos(start_lat) * cos(end_lat) * cos(start_lon - end_lon))

        # Return distance in meters
        return distance * 1000

    def get_nearby_relief_places(self, center_lat, center_long, radius):
        # Create a list to insert nearby places
        near_by_relief_places = []

        # Try to get all relief places from the KB
        relief_places = self.webgenesis_client.get_relief_places()

        # If fetch failed or returned empty list
        if not relief_places:
            print("No places of relief where found in the KB")
        else:
            for place in relief_places:
                if self.calculate_distance_between_locations(place["lat"], place["long"], center_lat,
                                                             center_long) <= radius:
                    near_by_relief_places.append(place)

        return near_by_relief_places

    def create_place_of_relief_message(self, incident_id, name, lat, long):
        message = {}

        message["header"] = {
            "topicName": "TOP101_INCIDENT_REPORT",
            "topicMajorVersion": 1,
            "topicMinorVersion": 0,
            "msgIdentifier": "kbs_msg_id_" + incident_id,
            "status": "Actual",
            "actionType": "Alert",
            "specificSender": "KBS_relief_places_locator",
            "scope": "Restricted",
            "district": "Thessaloniki",
            "recipients": "",
            "code": 0,
            "note": "",
            "references": ""
        }

        message["body"] = {
            "incidentOriginator": "KB",
            "incidentCategory": "Rescue",
            "incidentID": "INC_kbs_place_of_relief_incident_id_" + incident_id,
            "language": "en-US",
            "startTimeUTC": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "title": "Place of relief: " + name,
            "position": {
                "latitude": lat,
                "longitude": long
            }
        }

        return message

    def create_place_of_relief_incident_report(self, relief_place_uri, incident_id, name):
        relief_place_location_url = self.webgenesis_client.get_location_of_relief_place(relief_place_uri)

        # Data dictionary
        data_dict = {}

        # Add location
        if relief_place_location_url is not None:
            data_dict["location_" + incident_id] = {
                "uri": relief_place_location_url
            }
        else:
            data_dict["location_" + incident_id] = {
                "type": "Location",
                "properties": {
                    "latitude": "unknown",
                    "longitude": "unknown"
                }
            }

        # Add incident report
        data_dict["incident_report_place_of_relief" + incident_id] = {
            "type": "IncidentReport",
            "properties": {
                "hasReportID": "INC_kbs_place_of_relief_incident_id_" + incident_id,
                "hasPSAPIncidentID": "INC_kbs_place_of_relief_incident_id_" + incident_id,
                "hasReportLocation": "location_" + incident_id,
                "hasReportText": name
            }
        }

        # Query dictionary
        query_dict = {
            "data": data_dict,
            "defaultprefix": "http://beaware-project.eu/beAWARE/#"
        }

        self.insert_into_webgenesis(json.dumps(query_dict, indent=3))

        print(">> Place of relief populated to KB")

    def create_place_of_relief_capacity_104_message(self, place_of_relief_psap_incident_id, place_of_relief_uri):
        message = {}

        place_of_relief_name = self.webgenesis_client.get_object(
            subject='<' + place_of_relief_uri + '>',
            predicate="baw:instanceDisplayName"
        )

        place_of_relief_location = self.webgenesis_client.get_location_of_incident_report(
            place_of_relief_psap_incident_id)

        capacity = self.webgenesis_client.get_place_of_relief_capacity(place_of_relief_uri)
        capacity_used = self.webgenesis_client.get_place_of_relief_capacity_used(place_of_relief_uri)

        message["header"] = {
            "topicName": "TOP104_METRIC_REPORT",
            "topicMajorVersion": "0",
            "topicMinorVersion": "1",
            "status": "Actual",
            "actionType": "Update",
            "specificSender": "",
            "scope": "Public",
            "district": "",
            "recipients": "",
            "code": 20190617001,
            "note": "",
            "references": ""
        }

        message["body"] = {
            "dataStreamGenerator": "KBS",
            "dataStreamID": "HWAS_3101_PoR",
            "dataStreamName": "Current Occupancy of the PoR",
            "dataStreamDescription": "Availiable space of the places of relief",
            "language": "en-US",
            "dataStreamCategory": "Safety",
            "dataStreamSubCategory": "Other",
            "position": {
                "longitude": place_of_relief_location['long'],
                "latitude": place_of_relief_location['lat']
            },
            "measurements": [
                {
                    "measurementID": randint(1, 999999999),
                    "measurementTimeStamp": self.utc_now(),
                    "dataSeriesID": place_of_relief_psap_incident_id,
                    "dataSeriesName": place_of_relief_name,
                    "xValue": capacity,
                    "yValue": capacity_used,
                    "color": "#FFFFF0",  # TODO
                    "note": "Moderate"  # TODO
                }
            ]
        }

        return message

    def update_place_of_relief_capacity(self, incident_id):
        """Checks if a given incident is in a place of relief.
        If yes, it checks if the incident involves humans.
        If it involves humans, it updates the used capacity of the corresponding place of relief."""

        psap_incident_id = self.webgenesis_client.get_incident_report_psap_id(incident_id)

        # Check if this PSAP id is a place of relief incident report cluster and get its URI
        corresponding_place_of_relief_uri = self.webgenesis_client.get_place_of_relief_uri_from_psap_id(
            psap_incident_id)

        # If a correspondence between this PSAP id and a place of relief was actually found
        if corresponding_place_of_relief_uri is not None:

            # Get involved participants from the incident
            incident_participants = self.webgenesis_client.get_involved_participants_of_incident(
                self.webgenesis_client.get_incident_uri(incident_id)
            )

            # Count humans involved
            humans = 0
            for participant in incident_participants:
                if participant['type'] == "Human":
                    humans += 1

            # Update place of relief capacity used
            self.webgenesis_client.update_place_of_relief_capacity_used(corresponding_place_of_relief_uri, humans)

            # Inform the PSAP for new values
            self.produce_message(
                topic="TOP104_METRIC_REPORT",
                message=self.create_place_of_relief_capacity_104_message(
                    place_of_relief_psap_incident_id=psap_incident_id,
                    place_of_relief_uri=corresponding_place_of_relief_uri
                ),
                sender="KBS_METRICS"
            )
            print(">> TOP104 Metric report with PoR capacity was sent to PSAP")

    def insert_into_webgenesis(self, json_query):
        self.webgenesis_client.add_abox_data(json_query)

    def run_reasoning(self):
        self.reasoning_rule_1()
        self.reasoning_rule_2()

        print(">> Reasoning performed")

    def reasoning_rule_1(self):
        """
        An incident of type Flood, Fire, etc (not type Other) that involves Human vulnerable objects
        should be of High severity.
        """
        # TODO: determine if we need severe/extreme/something else (e.g. keep it)
        # return
        # self.webgenesis_client.update_incident_severity(incident_uri, "severe")

        query = """
                SELECT ?incident (group_concat(?severity_value;separator="|") as ?severities) 
                WHERE {
                    ?incident rdf:type baw:Incident .

                    ?participant baw:participantIsInvolvedIn ?incident .
                    ?participant rdf:type baw:Human .
                    
                    OPTIONAL {
                        ?incident baw:hasIncidentSeverity ?severity_value .
                    } .

                } GROUP BY ?incident
                """

        # MINUS {
        #     ?incident baw:hasIncidentSeverity "severe" .
        # }
        # BIND(IF(BOUND(?severity_value), ?severity_value, "unknown") AS ?severity) .
        # print("Rule 1")
        # print(query)
        #     ?incident baw:hasIncidentSeverity "extreme" .
        # ?incident baw:hasIncidentSeverity "severe" .
        results = self.webgenesis_client.execute_sparql_select(query)

        # print("Rule 1" + str(results))
        # print("cluster_type:" + str(cluster_type))

        # print(results)
        if results is not None:
            for result in results['results']['bindings']:
                # cluster_type = wg_client.get_incident_category(
                #     self.webgenesis_client.get_incident_report_psap_id(incident_id))

                cluster_type = "Other"
                if "severities" in result and result['severities']['value']:
                    previous_severities = result['severities']['value'].split("|")
                else:
                    previous_severities = []
                incident_uri = result['incident']['value']
                cluster_type = self.webgenesis_client.get_cluster_type_from_incident_uri(incident_uri)
                # psap_id = wg_client.get_incident_report_id(incident_uri)
                # psap_id
                # print("INCIDENT:" + str(incident_uri))
                if cluster_type != "Other" and "severe" not in previous_severities and "extreme" not in previous_severities:
                    # if "severe" not in previous_severities and "extreme" not in previous_severities:
                    print("RULE 1:" + str(previous_severities) + "," + str(cluster_type) + ", " + str(incident_uri))
                    print("Severity Update:" + str(previous_severities) + " ---------> " + str("severe"))
                    # Update incident severity value
                    # self.webgenesis_client.update_incident_severity(incident_uri, "severe")
                    self.webgenesis_client.add_incident_severity(incident_uri, "severe")
                    # if "current_level" in result and result['current_level']['value'] !="severe" and result['current_level']['value'] !="extreme":

    def reasoning_rule_2(self):
        """
        An incident of type Full or Empty without previous severity value should have severity "severe" or "minor".
        The incident should also have a registered participant (i.e. vulnerable object)
        """

        query = """
                SELECT DISTINCT ?incident ?incident_type_label
                WHERE {
                    ?incident rdf:type baw:Incident .
                    ?incident baw:isOfIncidentType ?incident_type .
                    ?incident_type rdfs:label ?incident_type_label .
                     
                    { ?incident baw:isOfIncidentType baw:Full }
                    UNION
                    { ?incident baw:isOfIncidentType baw:Empty } .
                    
                    MINUS {
                        ?incident baw:hasIncidentSeverity ?previous_severity_value .
                    }
                    
                    ?participant baw:participantIsInvolvedIn ?incident .
                    
                    
                    ?incident_report rdf:type baw:IncidentReport .                    
                    {
                        ?incident_report baw:hasAttachment ?attachment .
                    }
                    UNION
                    {
                        ?incident_report baw:hasDescription ?attachment .
                    }

                    ?task baw:relatesToMediaItem ?attachment .
                    ?task baw:taskProducesDataset ?dataset .

                    {
                        ?dataset baw:detectsDatasetIncident ?incident .
                    }
                    UNION
                    {
                        ?detection baw:isDetectionOf ?dataset .
                        ?detection baw:detectsIncident ?incident .
                    }
                    UNION
                    {
                        ?dataset baw:containsDetection ?detection .
                        ?detection baw:detectsIncident ?incident .
                    } .
                    
                    ?incident_report baw:hasPSAPIncidentID ?psap_id .
                    FILTER (STRSTARTS(?psap_id, "INC_kbs_place_of_relief_incident_id_"))
                }
                """

        results = self.webgenesis_client.execute_sparql_select(query)

        if results is not None:
            for result in results['results']['bindings']:
                incident_uri = result['incident']['value']
                incident_type = result['incident_type_label']['value']

                # Update incident severity value appropriately if Empty or Full
                if incident_type == 'Full':
                    self.webgenesis_client.update_incident_severity(incident_uri, "severe")
                elif incident_type == 'Empty':
                    self.webgenesis_client.update_incident_severity(incident_uri, "minor")


if __name__ == '__main__':
    import load_credentials

    with open("TOP019_modified.json", 'r') as f:
        inmess = json.load(f)

    reasoner = Reasoner(load_credentials.LoadCredentials.load_wg_credentials(), inmess[3])

    from loggers import query_logger

    query_logger.QueryLogger.flush_entries()
