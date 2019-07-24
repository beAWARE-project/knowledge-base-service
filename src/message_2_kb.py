import json
from webgenesis_client import WebGenesisClient
import requests
import time
from loggers.time_logger import TimeLogger


class Message2KB:
    @TimeLogger.timer_decorator(tags=["message2KB"])
    def __init__(self, webgenesis_conf, message_json):
        self.conf = webgenesis_conf
        self.webgenesis_client = WebGenesisClient(self.conf)

        self.message = message_json
        self.topic = self.message['header']['topicName']

        # Run the corresponding topic function
        try:
            getattr(self, self.topic.lower())()
        except:
            pass

    @TimeLogger.timer_decorator(tags=["top021"])
    def top021_incident_report(self):

        # If an Alert from app
        if (self.message['header']['actionType'] == 'Alert') and (self.message['header']['status'] == 'Actual'):

            # Handle alert
            self.top021_incident_report_app_alert()

        # If an Update
        elif (self.message['header']['actionType'] == 'Update') and (self.message['header']['status'] == 'Actual'):

            # Handle Update
            self.top021_incident_report_app_update()

    @TimeLogger.timer_decorator(tags=["top021"])
    def top021_incident_report_app_alert(self):
        print(">> TOP021 Incident alert received from APP")

        try:
            incident_id = self.message['body']['incidentID']
            position_latitude = self.message["body"]["position"]["latitude"]
            position_longitude = self.message["body"]["position"]["longitude"]
        except Exception as e:
            print("Error 1 @ Message2KB.top021_incident_report_app_alert")
            print("Error in message:\n", e)
            return

        # Data dictionary
        data_dict = {}

        # Add location
        data_dict["location_" + incident_id] = {
            "type": "Location",
            "properties": {
                "latitude": position_latitude,
                "longitude": position_longitude
            }
        }

        # Add incident report
        data_dict["incident_report_" + incident_id] = {
            "type": "IncidentReport",
            "properties": {
                "hasReportID": incident_id,
                "hasReportLocation": "location_" + incident_id,
                "hasReportText": json.dumps(self.message, indent=3),
                "incidentReportTimeStamp": self.message["body"]["startTimeUTC"]
            }
        }

        # Add originator (if given)
        if self.message["body"]["incidentOriginator"] and self.message["body"]["incidentOriginator"] != '':
            data_dict["incident_report_" + incident_id]["properties"]["hasOriginator"] = self.message["body"][
                "incidentOriginator"]

        # Add description (if given) as text item
        if ("description" in self.message["body"]) \
                and (self.message["body"]["description"] is not None) \
                and (self.message["body"]["description"]) \
                and (self.message["body"]["incidentOriginator"] in ['SCAPP', 'FRAPP']):
            data_dict["description_" + incident_id] = {
                "type": "TextItem",
                "properties": {
                    "hasRawMediaSource": self.message["body"]["description"],
                    "hasMediaItemTimestamp": self.message["body"]["startTimeUTC"],
                    "hasMediaItemName": "Description from incident report " + incident_id
                }
            }

            # Add description to incident report
            data_dict["incident_report_" + incident_id]["properties"]["hasDescription"] = "description_" + incident_id

        # Query dictionary
        query_dict = {
            "data": data_dict,
            "defaultprefix": "http://beaware-project.eu/beAWARE/#"
        }

        self.insert_into_webgenesis(json.dumps(query_dict, indent=3))

        print(">> Incident alert populated to KB")

    @TimeLogger.timer_decorator(tags=["top021"])
    def top021_incident_report_app_update(self):
        print(">> TOP021 Incident update received from APP")

        try:
            incident_id = self.message['body']['incidentID']
            attachments = self.message['body']['attachments']
        except Exception as e:
            print("Error 1 @ Message2KB.top021_incident_report_app_update")
            print("Error in message:\n", e)
            return

        # Get incident report instance uri
        incident_report_uri = self.webgenesis_client.get_incident_report_uri(incident_id)
        if incident_report_uri is None:
            print("Incident report not found in WebGenesis. Aborting insert to WebGenesis...")
            return

        # Update attachment names first
        try:
            originator = self.webgenesis_client.get_incident_report_originator(incident_id)

            if originator == 'SCAPP' or originator == 'FRAPP':
                originator_string = "mobile application"

            elif originator == 'VRS':
                originator_string = "static camera"

            else:
                originator_string = 'undefined source'

            for attachment in attachments:
                attachment["attachmentName"] = str(
                    attachment["attachmentType"]).title() + " from " + originator_string + " (" + str(
                    int(time.time())) + ")"

        except Exception as e:
            print("Error 2 @ Message2KB.top021_incident_report_app_update")
            print(e)

        # Update incident report text
        self.webgenesis_client.update_incident_report_text_in_sqlite(incident_id, json.dumps(self.message, indent=3))

        # Data dictionary
        data_dict = {}

        # For each attachment
        for index, attachment in enumerate(attachments):

            # If attachment type is empty, do nothing
            if attachment["attachmentType"] == "":
                print("Attachment type was not defined.")
                return

            data_dict["attachment_" + incident_id + "_" + str(index)] = {
                "type": self.classify_attachment_media_type(attachment["attachmentType"]),
                "properties": {
                    "hasRawMediaSource": attachment["attachmentURL"],
                    "hasMediaItemTimestamp": attachment["attachmentTimeStampUTC"],
                    "hasMediaItemName": attachment["attachmentName"],
                    "isAttachmentOf": "incident_report_" + incident_id
                }
            }

        # Select incident by URI
        data_dict["incident_report_" + incident_id] = {
            "uri": incident_report_uri,
        }

        # Query dictionary
        query_dict = {
            "data": data_dict,
            "defaultprefix": "http://beaware-project.eu/beAWARE/#"
        }

        self.insert_into_webgenesis(json.dumps(query_dict, indent=3))

        print(">> Incident update populated to KB")

    @TimeLogger.timer_decorator(tags=["top018"])
    def top018_image_analyzed(self):
        print(">> TOP018 image analysis received from HUB")

        try:
            incident_id = self.message['body']['incidentID']
            results_file_url = self.message['body']['im_analysis']
            analyzed_file_url = self.message['body']['im_analyzed']
        except Exception as e:
            print("Error 1 @ Message2KB.top018_image_analyzed")
            print("Error in message:\n", e)
            return

        # Data dictionary
        data_dict = {}

        # Get analysis results from json file to data dictionary
        try:
            results_json = json.loads(requests.get(results_file_url).text)

            # Add dataset incident
            data_dict["dataset_incident_" + incident_id] = {
                "type": "Incident",
                "properties": {
                    "isOfIncidentType": "incident_type",
                    "hasIncidentSeverity": results_json["image"]["crisis_level"],
                    "hasIncidentPriority": "undefined"
                }
            }

            # Select incident type by URI
            data_dict["incident_type"] = {
                "uri": self.classify_incident_type(results_json["image"]["crisis_type"])
            }

            # Get timestamp for all detections
            timestamp = results_json["image"]["timestamp"]

            # For each detection
            target_counter = 0
            detections_list = []
            for target in results_json["image"]["target"]:
                target_id = incident_id + "_" + str(target_counter)

                # Add vulnerable object
                data_dict["vulnerable_object_" + target_id] = {
                    "type": self.classify_vulnerable_object(target["type"]),
                    "properties": {
                        "participantIsInvolvedIn": "dataset_incident_" + incident_id
                    }
                }

                # Add detection
                data_dict["detection_" + target_id] = {
                    "type": "Detection",
                    "properties": {
                        "detectsParticipant": "vulnerable_object_" + target_id,
                        "hasDetectionConfidence": target["confidence"],
                        "hasDetectionRisk": target["risk"],
                        "hasDetectionTimestamp": timestamp
                    }
                }

                target_counter += 1
                detections_list.append("detection_" + target_id)

            # Add dataset
            data_dict["dataset_" + incident_id] = {
                "type": "DataSet",
                "properties": {
                    "detectsDatasetIncident": "dataset_incident_" + incident_id,
                    "hasDatasetResultsSource": results_file_url
                }
            }

            if detections_list:
                data_dict["dataset_" + incident_id]["properties"]["containsDetection"] = detections_list

            # Add image analysis task
            data_dict["image_analysis_" + incident_id] = {
                "type": "ImageAnalysis",
                "properties": {
                    "relatesToMediaItem": "attachment_" + incident_id,
                    "taskProducesDataset": "dataset_" + incident_id
                }
            }

        except Exception as e:
            print("Error 2 @ Message2KB.top018_image_analyzed")
            print(e)
            print("Result file could not be read.")
            print(results_file_url)

        # Add attachment properties
        data_dict["attachment_" + incident_id] = {
            "uri": self.webgenesis_client.get_attachments_of_incident_report(incident_id)[0],
            "properties": {
                "hasAnalyzedMediaSource": analyzed_file_url
            }
        }

        # Query dictionary
        query_dict = {
            "data": data_dict,
            "defaultprefix": "http://beaware-project.eu/beAWARE/#"
        }

        self.insert_into_webgenesis(json.dumps(query_dict, indent=3))

        print(">> Image analysis populated to KB")

    @TimeLogger.timer_decorator(tags=["top017"])
    def top017_video_analyzed(self):
        print(">> TOP017 video analysis received from HUB")

        try:
            incident_id = self.message['body']['incidentID']
            results_file_url = self.message['body']['vid_analysis']
            analyzed_file_url = self.message['body']['vid_analyzed']
        except Exception as e:
            print("Error 1 @ Message2KB.top017_video_analyzed")
            print("Error in message:\n", e)
            return

        # Data dictionary
        data_dict = {}

        # Get analysis results from json file to data dictionary
        try:
            results_json = json.loads(requests.get(results_file_url).text)

            # Add dataset incident
            data_dict["dataset_incident_" + incident_id] = {
                "type": "Incident",
                "properties": {
                    "isOfIncidentType": "incident_type",
                    "hasIncidentSeverity": results_json["sequence"]["crisis_level"],
                    "hasIncidentPriority": "undefined"
                }
            }

            # Select incident type by URI
            data_dict["incident_type"] = {
                "uri": self.classify_incident_type(results_json["sequence"]["crisis_type"])
            }

            # For each detection
            target_counter = 0
            detections_list = []
            for target in results_json["sequence"]["targets"]:
                target_id = incident_id + "_" + str(target_counter)

                # Add vulnerable object
                data_dict["vulnerable_object_" + target_id] = {
                    "type": self.classify_vulnerable_object(target["type"]),
                    "properties": {
                        "participantIsInvolvedIn": "dataset_incident_" + incident_id
                    }
                }

                # Add detection
                data_dict["detection_" + target_id] = {
                    "type": "Detection",
                    "properties": {
                        "detectsParticipant": "vulnerable_object_" + target_id,
                        "hasDetectionConfidence": self.calculate_average_confidence_score(
                            [box["confidence"] for box in target["trajectory"]]),
                        "hasDetectionStart": "'" + target["starts"] + "'^^xsd:dateTime",
                        # DONE: Created property in ontology
                        "hasDetectionEnd": "'" + target["ends"] + "'^^xsd:dateTime",
                        # DONE: Created property in ontology
                        "hasDetectionRisk": target["risk"],
                    }
                }

                target_counter += 1
                detections_list.append("detection_" + target_id)

            # Add dataset
            data_dict["dataset_" + incident_id] = {
                "type": "DataSet",
                "properties": {
                    "detectsDatasetIncident": "dataset_incident_" + incident_id,
                    "hasDatasetResultsSource": results_file_url
                }
            }

            if detections_list:
                data_dict["dataset_" + incident_id]["properties"]["containsDetection"] = detections_list

            # Add video analysis
            data_dict["video_analysis_" + incident_id] = {
                "type": "VideoAnalysis",
                "properties": {
                    "relatesToMediaItem": "attachment_" + incident_id,
                    "taskProducesDataset": "dataset_" + incident_id
                }
            }

        except Exception as e:
            print("Error 2 @ Message2KB.top017_video_analyzed")
            print(e)
            print("Result file could not be read.")
            print(results_file_url)

        # Add attachment properties
        data_dict["attachment_" + incident_id] = {
            "uri": self.webgenesis_client.get_attachments_of_incident_report(incident_id)[0],
            "properties": {
                "hasAnalyzedMediaSource": analyzed_file_url
            }
        }

        # Query dictionary
        query_dict = {
            "data": data_dict,
            "defaultprefix": "http://beaware-project.eu/beAWARE/#"
        }

        self.insert_into_webgenesis(json.dumps(query_dict, indent=3))

        print(">> Video analysis populated to KB")

    @TimeLogger.timer_decorator(tags=["top028"])
    def top028_text_analysed(self):
        print(">> TOP028 text analysis received from MTA")

        try:
            incident_id = self.message['body']['incidentID']
            analysis_results = self.message['body']['data']
        except Exception as e:
            print("Error 1 @ Message2KB.top028_text_analysed")
            print("Error in message:\n", e)
            return

        # If no findings in text analysis (data field is empty), return
        if not analysis_results:
            print("No analysis results found in TOP028")
            return

        # Data dictionary
        data_dict = {}

        # Get labels of incident types from WG to check if targets are incidents
        incident_types = self.webgenesis_client.get_incident_type_labels()

        # Initialize a list to append all vulnerable object detections
        detections_list = []

        # For each detection
        for key in analysis_results:

            target_id = key
            target = analysis_results[key]

            # Add detection
            data_dict["detection_" + target_id] = {
                "type": "Detection",
                "properties": {}
            }

            # Check if the target type matches with an incident type (flood, fire, etc.)
            incident_type = self.find_first_common_element(target["type"], incident_types)

            # If target type was found to be some incident type
            if incident_type is not None:
                data_dict["incident_" + target_id] = {
                    "type": "Incident",
                    "properties": {
                        "isOfIncidentType": "incident_type_" + target_id
                    }
                }

                # TODO: Add incident location, when the MTA starts discovering locations

                # Add incident to detection
                data_dict["detection_" + target_id]["properties"]["detectsIncident"] = "incident_" + target_id

                # Select incident type by URI
                data_dict["incident_type_" + target_id] = {
                    "uri": "http://beaware-project.eu/beAWARE/#" + incident_type
                }

                # If there are vulnerable objects as participants of incident
                if "participants" in target and target["participants"]:
                    participant_list = []
                    for participant in target["participants"]:
                        participant_list.append("vulnerable_object_" + participant["participant"])

                    data_dict["incident_" + target_id]["properties"]["involvesParticipant"] = participant_list

                # Check for given label
                if 'label' in target and target["label"] is not None and target["label"] != "null":
                    data_dict["incident_" + target_id]["properties"]["instanceDisplayName"] = target["label"]

                # Check for reference codes list and add if found
                if 'refs' in target and target["refs"]:
                    data_dict["incident_" + target_id]["properties"]["hasReferenceCode"] = target["refs"]

            # Else if the target is a vulnerable object
            else:
                # Add vulnerable object
                data_dict["vulnerable_object_" + target_id] = {
                    "type": target["type"][0],
                    "properties": {
                    }
                }

                # Add participant to detection
                data_dict["detection_" + target_id]["properties"][
                    "detectsParticipant"] = "vulnerable_object_" + target_id

                # Check for given label
                if 'label' in target and target["label"] is not None:
                    data_dict["vulnerable_object_" + target_id]["properties"]["instanceDisplayName"] = target["label"]

                # Check for given quantity
                if 'number' in target and target["number"] is not None:
                    data_dict["vulnerable_object_" + target_id]["properties"]["hasTextAnalysisQuantity"] = target[
                        "number"]

                # Check for given Role in any of detected incidents
                role = None
                for key_2 in analysis_results:
                    if ('participants' in analysis_results[key_2]) and (
                            analysis_results[key_2]['participants'] is not None):
                        for participant_2 in analysis_results[key_2]['participants']:
                            if (participant_2['participant'] == target_id) and ('role' in participant_2):
                                role = participant_2['role']

                # If a role was found
                if role is not None:
                    data_dict["vulnerable_object_" + target_id]["properties"]["hasTextAnalysisRole"] = role

                # Check for reference codes list and add if found
                if 'refs' in target and target["refs"]:
                    data_dict["vulnerable_object_" + target_id]["properties"]["hasReferenceCode"] = target["refs"]

            # Append detection to detections list
            detections_list.append("detection_" + target_id)

        # Add dataset
        data_dict["dataset_" + incident_id] = {
            "type": "DataSet",
            "properties": {
            }
        }

        # If any vulnerable objects were found among targets, add them to dataset
        if detections_list:
            data_dict["dataset_" + incident_id]["properties"]["containsDetection"] = detections_list

        # Add text analysis task
        data_dict["text_analysis_" + incident_id] = {
            "type": "TextAnalysis",
            "properties": {
                "relatesToMediaItem": "attachment_" + incident_id,
                "taskProducesDataset": "dataset_" + incident_id
            }
        }

        # Check if incident report has an attachment
        attachment_uri_list = self.webgenesis_client.get_attachments_of_incident_report(incident_id)
        if (attachment_uri_list is not None) and attachment_uri_list:
            attachment_uri = attachment_uri_list[0]

        # If no attachment was found
        else:
            # Get description text items of incident report
            description_uri_list = self.webgenesis_client.get_description_items_of_incident_report(incident_id)

            # If a description was found
            if (description_uri_list is not None) and description_uri_list:

                # Associate analysis results to it
                attachment_uri = description_uri_list[0]

            # If no description was found
            else:
                # Dismiss analysis as it cannot be associated to any item
                print("No attachment or description was found to associate analysis results from TOP028. Aborting...")
                return

        # Add attachment
        data_dict["attachment_" + incident_id] = {
            "uri": attachment_uri,
            "properties": {}
        }

        # Query dictionary
        query_dict = {
            "data": data_dict,
            "defaultprefix": "http://beaware-project.eu/beAWARE/#"
        }

        self.insert_into_webgenesis(json.dumps(query_dict, indent=3))

        print(">> Text analysis populated to KB")

    @TimeLogger.timer_decorator(tags=["top040"])
    def top040_text_report_generated(self):
        print(">> TOP040 generated report received from MRG")

        try:
            incident_id = self.message['body']['incidentID']
            description = self.message['body']['description']
            title = self.message['body']['title']
        except Exception as e:
            print("Error 1 @ Message2KB.top040_text_report_generated")
            print("Error in message:\n", e)
            return

        # TODO: Decide if needed to create hasTitle, hasDescription properties in ontology and instantiate them with these values

    @TimeLogger.timer_decorator(tags=["top001"])
    def top001_social_media_text(self):
        print(">> TOP001 new social media text arrived from SMA")

        try:
            incident_id = self.message['body']['incidentID']
            text = self.message["body"]["description"]
        except Exception as e:
            print("Error 1 @ Message2KB.top001_social_media_text")
            print("Error in message:\n", e)
            return

        if "position" in self.message["body"]:
            position_latitude = self.message["body"]["position"]["latitude"]
            position_longitude = self.message["body"]["position"]["longitude"]
        else:
            position_latitude = "undefined"
            position_longitude = "undefined"

        # Data dictionary
        data_dict = {}

        # Add location
        data_dict["location_" + incident_id] = {
            "type": "Location",
            "properties": {
                "latitude": position_latitude,
                "longitude": position_longitude
            }
        }

        # Add incident report
        data_dict["incident_report_" + incident_id] = {
            "type": "IncidentReport",
            "properties": {
                "hasReportID": incident_id,
                "hasReportLocation": "location_" + incident_id,
                "hasReportText": json.dumps(self.message, indent=3),
                "incidentReportTimeStamp": self.message["body"]["startTimeUTC"]
            }
        }

        # Add attachment
        data_dict["attachment_" + incident_id] = {
            "type": "TextItem",
            "properties": {
                "hasRawMediaSource": text,
                "hasMediaItemTimestamp": self.message["body"]["startTimeUTC"],
                "hasMediaItemName": incident_id,
                "isAttachmentOf": "incident_report_" + incident_id
            }
        }

        # Query dictionary
        query_dict = {
            "data": data_dict,
            "defaultprefix": "http://beaware-project.eu/beAWARE/#"
        }

        self.insert_into_webgenesis(json.dumps(query_dict, indent=3))

        print(">> Social media text populated to KB")

    @TimeLogger.timer_decorator(tags=["top003"])
    def top003_social_media_report(self):
        print(">> TOP003 new social media report arrived from SMC")

        try:
            incident_id = self.message['body']['incidentID']
            position_latitude = self.message["body"]["position"]["latitude"]
            position_longitude = self.message["body"]["position"]["longitude"]
            attachments = self.message['body']['attachments']
        except Exception as e:
            print("Error 1 @ Message2KB.top003_social_media_report")
            print("Error in message:\n", e)
            return

        # Data dictionary
        data_dict = {}

        # Add location
        data_dict["location_" + incident_id] = {
            "type": "Location",
            "properties": {
                "latitude": position_latitude,
                "longitude": position_longitude
            }
        }

        # Add incident report
        data_dict["incident_report_" + incident_id] = {
            "type": "IncidentReport",
            "properties": {
                "hasReportID": incident_id,
                "hasReportLocation": "location_" + incident_id,
                "hasReportText": json.dumps(self.message, indent=3),
                "incidentReportTimeStamp": self.message["body"]["startTimeUTC"]
            }
        }

        # For each attachment
        for index, attachment in enumerate(attachments):
            data_dict["attachment_" + incident_id + "_" + str(index)] = {
                "type": "HTMLItem",
                "properties": {
                    "hasRawMediaSource": attachment["attachmentURL"],
                    "hasMediaItemTimestamp": attachment["attachmentTimeStampUTC"],
                    "hasMediaItemName": "Twitter report (" + str(int(time.time())) + ")",
                    "isAttachmentOf": "incident_report_" + incident_id
                }
            }

        # Query dictionary
        query_dict = {
            "data": data_dict,
            "defaultprefix": "http://beaware-project.eu/beAWARE/#"
        }

        self.insert_into_webgenesis(json.dumps(query_dict, indent=3))

        print(">> Social media report populated to KB")

    @TimeLogger.timer_decorator(tags=["top019"])
    def top019_uav_media_analyzed(self):
        print(">> TOP019 UAV media analysis received")
        try:
            incident_id = self.message["body"]["incidentID"]
            incident_detected = self.message["body"]["incident_detected"]
            position_latitude = self.message["body"]["location"]["latitude"]
            position_longitude = self.message["body"]["location"]["longitude"]
            results_file_url = self.message['body']['media_analysis']
            analyzed_file_url = self.message['body']['media_analyzed']
            media_timestamp = self.message["body"]["media_timestamp"]
        except Exception as e:
            print("Error 1 @ Message2KB.top019_uav_media_analyzed")
            print("Error in message:\n", e)
            return

        if not incident_detected:
            return

        # Get analysis results from json file
        try:
            results_json = json.loads(requests.get(results_file_url).text)

            media_item_name = results_json["sequence"]["name"]
            crisis_type = results_json["sequence"]["crisis_type"]
            targets = results_json["sequence"]["targets"]
        except Exception as e:
            print("Error 2 @ Message2KB.top019_uav_media_analyzed")
            print("Could not retrieve analysis results json file:\n", results_file_url, e)
            return

        # Data dictionary
        data_dict = {}

        # Add location
        data_dict["location_" + incident_id] = {
            "type": "Location",
            "properties": {
                "latitude": position_latitude,
                "longitude": position_longitude
            }
        }

        # Add incident report
        data_dict["incident_report_" + incident_id] = {
            "type": "IncidentReport",
            "properties": {
                "hasReportID": incident_id,
                "hasReportLocation": "location_" + incident_id,
                "incidentReportTimeStamp": media_timestamp
            }
        }

        # Add attachment
        data_dict["attachment_" + incident_id] = {
            "type": "VideoItem",
            "properties": {
                "hasRawMediaSource": analyzed_file_url,
                "hasMediaItemTimestamp": media_timestamp,
                "hasMediaItemName": media_item_name,
                "isAttachmentOf": "incident_report_" + incident_id,
                "hasAnalyzedMediaSource": analyzed_file_url
            }
        }

        # Add dataset incident
        data_dict["dataset_incident_" + incident_id] = {
            "type": "Incident",
            "properties": {
                "isOfIncidentType": "incident_type",
                "hasIncidentSeverity": results_json["sequence"]["crisis_level"],
                "hasIncidentPriority": "undefined"
            }
        }

        # Select incident type by URI
        data_dict["incident_type"] = {
            "uri": self.classify_incident_type(crisis_type)
        }

        # Add dataset
        data_dict["dataset_" + incident_id] = {
            "type": "DataSet",
            "properties": {
                "detectsDatasetIncident": "dataset_incident_" + incident_id,
                "hasDatasetResultsSource": results_file_url
            }
        }

        # For each detection (target)
        target_counter = 0
        detections_list = []
        for target in targets:
            target_id = incident_id + "_" + str(target_counter)

            # Add vulnerable object
            data_dict["vulnerable_object_" + target_id] = {
                "type": self.classify_vulnerable_object(target["type"]),
                "properties": {
                    "participantIsInvolvedIn": "dataset_incident_" + incident_id
                }
            }

            # Add detection
            data_dict["detection_" + target_id] = {
                "type": "Detection",
                "properties": {
                    "detectsParticipant": "vulnerable_object_" + target_id,
                    "hasDetectionConfidence": self.calculate_average_confidence_score(
                        [box["confidence"] for box in target["trajectory"]]),
                    "hasDetectionStart": "'" + target["starts"] + "'^^xsd:dateTime",
                    "hasDetectionEnd": "'" + target["ends"] + "'^^xsd:dateTime",
                    "hasDetectionRisk": target["risk"],
                }
            }

            target_counter += 1
            detections_list.append("detection_" + target_id)

        if detections_list:
            data_dict["dataset_" + incident_id]["properties"]["containsDetection"] = detections_list

        # Add video analysis
        data_dict["video_analysis_" + incident_id] = {
            "type": "VideoAnalysis",
            "properties": {
                "relatesToMediaItem": "attachment_" + incident_id,
                "taskProducesDataset": "dataset_" + incident_id
            }
        }

        # Query dictionary
        query_dict = {
            "data": data_dict,
            "defaultprefix": "http://beaware-project.eu/beAWARE/#"
        }

        self.insert_into_webgenesis(json.dumps(query_dict, indent=3))

        print(">> Drone footage analysis populated to KB")


    @TimeLogger.timer_decorator(tags=["top006"])
    def top006_incident_report_crcl(self):
        print(">> TOP006 Incident alert received from CRCL")

        try:
            incident_id = self.message['body']['incidentID']
            position_latitude = self.message["body"]["position"]["latitude"]
            position_longitude = self.message["body"]["position"]["longitude"]
            incident_type = self.message['body']['incidentType']
            severity = self.message['body']['severity']
        except Exception as e:
            print("Error 1 @ Message2KB.top006_incident_report_crcl")
            print("Error in message:\n", e)
            return

        # Data dictionary
        data_dict = {}

        # Add location
        data_dict["location_" + incident_id] = {
            "type": "Location",
            "properties": {
                "latitude": position_latitude,
                "longitude": position_longitude
            }
        }

        # Add incident report
        data_dict["incident_report_" + incident_id] = {
            "type": "IncidentReport",
            "properties": {
                "hasReportID": incident_id,
                "hasReportLocation": "location_" + incident_id,
                "hasReportText": json.dumps(self.message, indent=3),
                "incidentReportTimeStamp": self.message["body"]["startTimeUTC"]
            }
        }

        # Add originator (if given)
        if self.message["body"]["incidentOriginator"] and self.message["body"]["incidentOriginator"] != '':
            data_dict["incident_report_" + incident_id]["properties"]["hasOriginator"] = self.message["body"][
                "incidentOriginator"]

        # Add description as text item
        data_dict["description_" + incident_id] = {
            "type": "TextItem",
            "properties": {
                "hasRawMediaSource": self.message["body"]["description"],
                "hasMediaItemTimestamp": self.message["body"]["startTimeUTC"],
                "hasMediaItemName": "Description from incident report " + incident_id
            }
        }

        # Add description to incident report
        data_dict["incident_report_" + incident_id]["properties"]["hasDescription"] = "description_" + incident_id

        # Add analysis task
        data_dict["analysis_" + incident_id] = {
            "type": "TextAnalysis",
            "properties": {
                "relatesToMediaItem": "description_" + incident_id,
                "taskProducesDataset": "dataset_" + incident_id
            }
        }

        # Add dataset
        data_dict["dataset_" + incident_id] = {
            "type": "DataSet",
            "properties": {
                "detectsDatasetIncident": "dataset_incident_" + incident_id
            }
        }

        # Add dataset incident
        data_dict["dataset_incident_" + incident_id] = {
            "type": "Incident",
            "properties": {
                "isOfIncidentType": "incident_type",
                "hasIncidentSeverity": severity,
                "hasIncidentPriority": "undefined"
            }
        }

        # Select incident type by URI
        data_dict["incident_type"] = {
            "uri": self.classify_incident_type(incident_type)
        }

        # Query dictionary
        query_dict = {
            "data": data_dict,
            "defaultprefix": "http://beaware-project.eu/beAWARE/#"
        }

        self.insert_into_webgenesis(json.dumps(query_dict, indent=3))

        print(">> Incident report from CRCL populated to KB")


    @TimeLogger.timer_decorator(tags=["top007"])
    def top007_update_incident_risk(self):
        print(">> TOP007 Incident risk update received from CRCL")

        try:
            incident_id = self.message['body']['incidentID']
            severity = self.message['body']['severity']
        except Exception as e:
            print("Error 1 @ Message2KB.top007_update_incident_risk")
            print("Error in message:\n", e)
            return

        # Insert incident report severity to WebGenesis
        self.webgenesis_client.set_incident_report_severity_calculated_by_crcl(
            report_id=incident_id,
            severity_value=severity
        )

    def insert_into_webgenesis(self, json_query):
        self.webgenesis_client.add_abox_data(json_query)

    def classify_attachment_media_type(self, attachment_type):
        attachment_type = attachment_type.lower()

        if attachment_type == "image":
            return "ImageItem"
        elif attachment_type == "video":
            return "VideoItem"
        elif attachment_type == "audio":
            return "AudioItem"
        elif attachment_type == "webpage":
            return "HTMLItem"
        else:
            return "MediaItem"

    def classify_vulnerable_object(self, obj):
        obj = obj.lower()

        objects_dictionary = {
            "car": "Car",
            "bicycle": "Bicycle",
            "truck": "Truck",
            "bus": "Bus",
            "boat": "Boat",
            "motorcycle": "Motorcycle",
            "train": "Vehicle",
            "person": "Human",
            "dummy": "Human",
            "dog": "Dog",
            "cat": "Cat",
            "wheelchairuser": "WheelchairUser"
        }

        try:
            print("vulnerable objects: "+str(objects_dictionary[obj]))
            return objects_dictionary[obj]
        except Exception as e:
            print(e)
            return ""

    def classify_incident_type(self, incident_type):
        incident_type = incident_type.lower()

        incidents_dictionary = {
            "fire": "Fire",
            "flood": "Flood",
            "traffic": "Traffic",
            "heat wave": "Heatwave",
            "overflow": "Overflow",
            "precipitation": "Precipitation",
            "collapse": "Collapse",
            "other": "OtherIncident",
            "incident": "OtherIncident",
            "smoke": "Smoke"
        }

        try:
            print("incident types: " +str(incidents_dictionary[incident_type]))
            return "http://beaware-project.eu/beAWARE/#" + incidents_dictionary[incident_type]
        except Exception as e:
            print(e)
            return "http://beaware-project.eu/beAWARE/#OtherIncident"

    def calculate_average_confidence_score(self, score_list):
        return sum(score_list) / float(len(score_list))

    def find_first_common_element(self, list_1, list_2):
        for item in list_1:
            if item in list_2:
                return item

        return None
