import requests
import json
import sqlite3
from datetime import datetime
from loggers.query_logger import QueryLogger
import time
import load_credentials


class WebGenesisClient:
    def __init__(self, webgenesis_conf):
        self.hostname = webgenesis_conf["hostname"]
        self.ontology_entry_id = webgenesis_conf["ontology_entry_id"]
        self.username = webgenesis_conf["username"]
        self.password = webgenesis_conf["password"]

        self.login_url = self.hostname + '/servlet/is/rest/login'  # E.g. 'http://172.17.0.2/servlet/is/rest/login'
        self.logout_url = self.hostname + '/servlet/is/rest/logout'  # E.g. 'http://172.17.0.2/servlet/is/rest/logout'
        self.addABoxData_url = self.hostname + '/servlet/is/rest/entry/' + str(self.ontology_entry_id) + '/addABoxData/'
        self.removeABoxData_url = self.hostname + '/servlet/is/rest/entry/' + str(
            self.ontology_entry_id) + '/removeABoxData/'
        self.sparql_url = self.hostname + '/servlet/is/Entry.' + str(self.ontology_entry_id) + '.SPARQLEndpoint/'

        # The sqlite database where report texts are stored
        self.database = 'messages.sqlite'

        # self.session = None
        self._severity_values = ["minor", "moderate", "severe", "extreme"]

        # Define prefixes for SPARQL
        self.prefixes = """
                            PREFIX baw: <http://beaware-project.eu/beAWARE/#>
                            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                        """

    def login(self):
        # print("-----------------IN LOGIN!--------------------")
        credentials = {'user': (None, self.username), 'key': (None, self.password)}

        # Create session handler
        self.session = requests.Session()

        # Send login request
        try:
            r = self.session.post(self.login_url, data=credentials)
            # print(r.text)
        except Exception as e:
            print(e)
            self.session = None
            return False

        # Return the request's status code and session handler
        return r.status_code

    def logout(self):
        headers = {'Accept': 'application/json'}

        try:
            # Send login request
            r = self.session.post(self.logout_url, headers=headers)

        except Exception as e:
            print("Logout failed")
            print(e)
            return False

        # If 200, success
        return r.status_code

    def add_abox_data(self, json_query):
        # If query argument is valid
        try:
            query = json.loads(json_query)
        except ValueError as e:
            print("Invalid query argument")
            print(e)
            return False

        # Login
        # if self.login() == 200:

        # Prepare headers
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        query_time = time.time()
        reply_dict = dict()
        reply_dict["status_code"] = None
        reply_dict["text"] = None
        reply_dict["url"] = None

        # Send request with query
        try:
            # print("{} before add_abox ".format(time.time()))
            r = self.session.post(self.addABoxData_url, headers=headers, data=json.dumps(query))
            # print("{} after add_abox ".format(time.time()))
            if type(r) is dict:
                if "status_code" in r and "text" in r and "url" in r:
                    reply_dict["status_code"] = r.status_code
                    reply_dict["text"] = r.text
                    reply_dict["url"] = r.url

            if type(r) is requests.models.Response:
                reply_dict["status_code"] = r.status_code
                reply_dict["text"] = r.text
                reply_dict["url"] = r.url

        except Exception as e:
            print('addABoxData failed at http request')
            print(e)
            return False
        finally:
            reply_time = time.time()
            QueryLogger.log_entry(label="add_abox_data", time_query=query_time, query_json=query,
                                  time_reply=reply_time, reply_json=reply_dict)

        # Logout
        # self.logout()
        #
        # else:
        #     print("Login failed")

    def remove_abox_data(self, json_query):

        # If query argument is valid
        try:
            query = json.loads(json_query)
        except ValueError as e:
            print("Invalid query argument")
            print(e)
            return False

        # Login
        # if self.login() == 200:
        # print("{} before remove abox ".format(time.time()))
        # Prepare headers
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        # print("{} after remove abox ".format(time.time()))

        query_time = time.time()
        reply_dict = dict()
        reply_dict["status_code"] = None
        reply_dict["text"] = None
        reply_dict["url"] = None

        # Send request with query
        try:
            r = self.session.post(self.removeABoxData_url, headers=headers, data=json.dumps(query))

            if type(r) is dict:
                if "status_code" in r and "text" in r and "url" in r:
                    reply_dict["status_code"] = r.status_code
                    reply_dict["text"] = r.text
                    reply_dict["url"] = r.url

            if type(r) is requests.models.Response:
                reply_dict["status_code"] = r.status_code
                reply_dict["text"] = r.text
                reply_dict["url"] = r.url

        except Exception as e:
            print('removeABoxData failed at http request')
            print(e)
            return False
        finally:
            reply_time = time.time()
            QueryLogger.log_entry(label="remove_abox_data", time_query=query_time, query_json=query,
                                  time_reply=reply_time, reply_json=reply_dict)

        # Logout
        # self.logout()

        # else:
        #     print("Login failed")

    def execute_sparql_select(self, query):
        # Add prefixes
        query = self.prefixes + query

        # Login
        # if self.login() == 200:

        headers = {'Content-type': 'application/x-www-form-urlencoded'}

        query_time = time.time()
        reply_dict = dict()
        reply_dict["status_code"] = None
        reply_dict["text"] = None
        reply_dict["url"] = None

        # Send request with query
        try:
            # print("{} before select ".format(time.time()))
            r = self.session.post(self.sparql_url, headers=headers, params={'query': query, 'output': 'json'})
            # print("{} after select ".format(time.time()))

            if type(r) is dict:
                if "status_code" in r and "text" in r and "url" in r:
                    reply_dict["status_code"] = r.status_code
                    reply_dict["text"] = r.text
                    reply_dict["url"] = r.url

            if type(r) is requests.models.Response:
                reply_dict["status_code"] = r.status_code
                reply_dict["text"] = r.text
                reply_dict["url"] = r.url

        except Exception as e:
            print('Select failed at SPARQL request')
            print(e)
            return False
        finally:
            reply_time = time.time()
            QueryLogger.log_entry(label="execute_sparql_select", time_query=query_time, query_json=query,
                                  time_reply=reply_time, reply_json=reply_dict)

        # Logout
        # self.logout()

        try:
            return json.loads(r.text)
        except Exception as e:
            print("SPARQL SELECT REPLY:" + str(r))
            print('Error @ execute_sparql_select: Failed to load SPARQL query result to JSON')
            print(e)
            return None

        # else:
        #     print("Login failed")

    def get_incident_report_uri(self, incident_id):
        return self.get_subject('baw:hasReportID', '"' + incident_id + '"')

    def get_incident_uri(self, incident_id):
        query = """
                SELECT ?incident
                WHERE {                 
                    {
                        ?dataset baw:detectsDatasetIncident ?incident . 
                    } 
                    UNION
                    {
                        ?detection baw:isDetectionOf ?dataset .
                        ?detection baw:detectsIncident ?incident .
                        ?dataset baw:detectsDatasetIncident ?incident .
                    } . 
                    
                    { 
                        ?task baw:taskProducesDataset ?dataset .
                    }
                    UNION
                    {
                        ?dataset baw:isProducedByTask ?task .
                    }
                    
                    {
                        ?task baw:relatesToMediaItem ?media_item .
                    }
                    UNION
                    {
                        ?media_item baw:relatesToTask ?task .
                    }
                    
                    {
                        ?incident_report baw:hasAttachment ?media_item .
                    }
                    UNION
                    {
                        ?media_item baw:isAttachmentOf ?incident_report .
                    }
                    
                    ?incident_report baw:hasReportID "%s" .                    
                }
                """ % (incident_id,)

        try:
            return self.execute_sparql_select(query)['results']['bindings'][0]['incident']['value']
        except:
            return None

    def get_incident_report_text(self, incident_id):
        query = """
            SELECT ?text
            WHERE {
                ?incident_report rdf:type baw:IncidentReport .
                ?incident_report baw:hasReportID "%s" .
                ?incident_report baw:hasReportText ?text .
            }
        """ % (incident_id,)

        try:
            return self.execute_sparql_select(query)['results']['bindings'][0]['text']['value']
        except:
            return None

    def update_incident_report_text(self, incident_id, new_text):
        incident_report_uri = self.get_incident_report_uri(incident_id)

        # Delete old text
        delete_query = {
            "defaultprefix": "http://beaware-project.eu/beAWARE/#",
            "data": {
                "individuals": [],
                "properties": {
                    incident_report_uri: {
                        "hasReportText": self.get_incident_report_text(incident_id)
                    }
                }
            }
        }

        self.remove_abox_data(json.dumps(delete_query))

        # Insert new text
        insert_query = {
            "defaultprefix": "http://beaware-project.eu/beAWARE/#",
            "data": {
                "incident": {
                    "uri": incident_report_uri,
                    "properties": {
                        "hasReportText": new_text
                    }
                }
            }
        }

        self.add_abox_data(json.dumps(insert_query))

    def get_incident_report_text_from_sqlite(self, incident_id):
        try:
            con = sqlite3.connect(self.database)

            cur = con.cursor()
            cur.execute("SELECT text FROM report_texts WHERE incident_report_id=?", (incident_id,))

            result = cur.fetchone()
            # print("sqlite query " + str(result))
            cur.close()

            return result[0]

        except Exception as e:
            print("Error @ WebGenesisClient.get_incident_report_text_from_sqlite(), incidentID: "+str(incident_id))
            print(e)
            return None

    def update_incident_report_text_in_sqlite(self, incident_id, new_text):
        try:
            con = sqlite3.connect(self.database)

            with con:
                cur = con.cursor()
                cur.execute("""INSERT OR REPLACE INTO report_texts (incident_report_id, text) VALUES(?, ?)""",
                            (incident_id, new_text,))

        except Exception as e:
            print("Error @ WebGenesisClient.update_incident_report_text_in_sqlite()")
            print(e)
            return None

    def delete_all_incident_report_texts_in_sqlite(self):
        try:
            con = sqlite3.connect(self.database)

            with con:
                cur = con.cursor()
                cur.execute("DELETE FROM report_texts WHERE 1")

        except Exception as e:
            print("Error @ WebGenesisClient.delete_all_incident_report_texts_in_sqlite()")
            print(e)
            return None

    def get_incident_report_psap_id(self, incident_id):

        query = """
                SELECT ?psap_id
                WHERE {
                    ?incident_report baw:hasReportID "%s".
                    ?incident_report baw:hasPSAPIncidentID ?psap_id .
                }
        """ % (incident_id,)

        try:
            return self.execute_sparql_select(query)['results']['bindings'][0]['psap_id']['value']
        except:
            return incident_id

    def set_incident_report_psap_id(self, incident_id, psap_incident_id):

        incident_report_uri = self.get_incident_report_uri(incident_id)

        # Insert PSAP incident id
        insert_query = {
            "defaultprefix": "http://beaware-project.eu/beAWARE/#",
            "data": {
                "incident": {
                    "uri": incident_report_uri,
                    "properties": {
                        "hasPSAPIncidentID": psap_incident_id
                    }
                }
            }
        }

        self.add_abox_data(json.dumps(insert_query))

    def get_incident_report_originator(self, incident_id):

        query = """
                SELECT ?originator
                WHERE {
                    ?incident_report baw:hasReportID "%s".
                    ?incident_report baw:hasOriginator ?originator .
                }
        """ % (incident_id,)

        try:
            return self.execute_sparql_select(query)['results']['bindings'][0]['originator']['value']
        except:
            return None

    def get_incident_priority(self, incident_uri):
        priority = self.get_object("<" + incident_uri + ">", "baw:hasIncidentPriority")
        # print("Priority: "+str(priority))
        return priority
        # return self.get_object("<" + incident_uri + ">", "baw:hasIncidentPriority")

    def get_incident_severity(self, incident_uri):
        sev = self.get_object("<" + incident_uri + ">", "baw:hasIncidentSeverity")
        return sev
        # return self.get_object("<" + incident_uri + ">", "baw:hasIncidentSeverity")

    def get_incident_report_severity(self, incident_report_id):
        query = """
                SELECT ?severity
                WHERE {
                    ?incident_report baw:hasReportID '%s' .

                    {
                        ?incident_report baw:hasAttachment ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:isAttachmentOf ?incident_report .
                    }
                    UNION
                    {
                        ?incident_report baw:hasDescription ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:isDescriptionOf ?incident_report .
                    }

                    { 
                        ?task baw:taskProducesDataset ?dataset .
                    }
                    UNION
                    {
                        ?dataset baw:isProducedByTask ?task .
                    }
                    
                    {
                        ?task baw:relatesToMediaItem ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:relatesToTask ?task .
                    }

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

                    OPTIONAL {
                        ?incident baw:hasIncidentSeverity ?severity_value .
                    } .

                    BIND(IF(BOUND(?severity_value), ?severity_value, "unknown") AS ?severity) .

                }
            """ % (incident_report_id,)

        try:
            results = self.execute_sparql_select(query)
            severities = [result['severity']['value'] for result in results['results']['bindings']]

        except Exception as e:
            print(e)
            return "unknown"

        # Also append to the severities list any value calculated by the CRCL
        severities.append(self.get_incident_report_severity_calculated_by_crcl(incident_report_id))

        if ("extreme" in severities) or ("Extreme" in severities):
            return "extreme"
        elif ("severe" in severities) or ("Severe" in severities):
            return "severe"
        elif ("moderate" in severities) or ("Moderate" in severities):
            return "moderate"
        elif ("minor" in severities) or ("Minor" in severities):
            return "minor"
        else:
            return "unknown"

    def get_incident_report_severity_calculated_by_crcl(self, incident_report_id):
        query = """
                SELECT ?severity
                WHERE {
                    ?incident_report baw:hasReportID '%s' .

                    OPTIONAL {
                        ?incident_report baw:hasSeverityFromCRCL ?severity_value .
                    } .

                    BIND(IF(BOUND(?severity_value), ?severity_value, "unknown") AS ?severity) .

                }
            """ % (incident_report_id,)

        try:
            results = self.execute_sparql_select(query)
            severities = [result['severity']['value'] for result in results['results']['bindings']]

        except Exception as e:
            print(e)
            return "unknown"

        if ("extreme" in severities) or ("Extreme" in severities):
            return "extreme"
        elif ("severe" in severities) or ("Severe" in severities):
            return "severe"
        elif ("moderate" in severities) or ("Moderate" in severities):
            return "moderate"
        elif ("minor" in severities) or ("Minor" in severities):
            return "minor"
        else:
            return "unknown"

    def get_incident_cluster_severity(self, psap_id):
        query = """
                SELECT ?severity
                WHERE {
                    ?incident_report baw:hasPSAPIncidentID '%s' .

                    {
                        ?incident_report baw:hasAttachment ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:isAttachmentOf ?incident_report .
                    }
                    UNION
                    {
                        ?incident_report baw:hasDescription ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:isDescriptionOf ?incident_report .
                    }

                    { 
                        ?task baw:taskProducesDataset ?dataset .
                    }
                    UNION
                    {
                        ?dataset baw:isProducedByTask ?task .
                    }
                    
                    {
                        ?task baw:relatesToMediaItem ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:relatesToTask ?task .
                    }

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

                    OPTIONAL {
                        ?incident baw:hasIncidentSeverity ?severity_value .
                    } .

                    BIND(IF(BOUND(?severity_value), ?severity_value, "unknown") AS ?severity) .

                }
            """ % (psap_id,)

        try:
            results = self.execute_sparql_select(query)
            severities = [result['severity']['value'] for result in results['results']['bindings']]
            # print("id: {},  severities: {}".format(psap_id, severities))

        except Exception as e:
            print(e)
            return "unknown"

        # Also append to the severities list any value calculated by the CRCL
        severity_crcl = self.get_incident_cluster_severity_calculated_by_crcl(psap_id)
        severities.append(severity_crcl)
        print("Severity by CRCL:"+str(severity_crcl)+" for id: "+str(psap_id))
        if severity_crcl != "unknown":
            return severity_crcl

        if ("extreme" in severities) or ("Extreme" in severities):
            return "extreme"
        elif ("severe" in severities) or ("Severe" in severities):
            return "severe"
        elif ("moderate" in severities) or ("Moderate" in severities):
            return "moderate"
        elif ("minor" in severities) or ("Minor" in severities):
            return "minor"
        else:
            return "unknown"

    def get_incident_cluster_severity_calculated_by_crcl(self, psap_id):
        query = """
                SELECT ?severity
                WHERE {
                    ?incident_report baw:hasPSAPIncidentID '%s' .

                    OPTIONAL {
                        ?incident_report baw:hasSeverityFromCRCL ?severity_value .
                    } .

                    BIND(IF(BOUND(?severity_value), ?severity_value, "unknown") AS ?severity) .

                }
            """ % (psap_id,)

        try:
            results = self.execute_sparql_select(query)
            severities = [result['severity']['value'] for result in results['results']['bindings']]
            print("Severities by crcl:"+str(severities))

        except Exception as e:
            print(e)
            return "unknown"

        if ("extreme" in severities) or ("Extreme" in severities):
            return "extreme"
        elif ("severe" in severities) or ("Severe" in severities):
            return "severe"
        elif ("moderate" in severities) or ("Moderate" in severities):
            return "moderate"
        elif ("minor" in severities) or ("Minor" in severities):
            return "minor"
        else:
            return "unknown"

    def update_incident_severity(self, incident_uri, new_value):

        print("severity update start", self.utc_now())

        try:
            # Delete old value
            delete_query = {
                "defaultprefix": "http://beaware-project.eu/beAWARE/#",
                "data": {
                    "individuals": [],
                    "properties": {
                        incident_uri: {
                            "hasIncidentSeverity": self.get_incident_severity(incident_uri)
                        }
                    }
                }
            }

            self.remove_abox_data(json.dumps(delete_query))

        except:
            pass

        # Insert new value
        insert_query = {
            "defaultprefix": "http://beaware-project.eu/beAWARE/#",
            "data": {
                "incident": {
                    "uri": incident_uri,
                    "properties": {
                        "hasIncidentSeverity": new_value
                    }
                }
            }
        }

        self.add_abox_data(json.dumps(insert_query))

        print("severity update end", self.utc_now())

    def set_incident_report_severity_calculated_by_crcl(self, report_id, severity_value):

        try:
            incident_report_uri = self.get_incident_report_uri(report_id)

        except Exception as e:
            print("Error 1 @ WebGenesisClient.set_incident_report_severity_calculated_by_crcl")
            print(e)
            return

        # Insert severity value
        insert_query = {
            "defaultprefix": "http://beaware-project.eu/beAWARE/#",
            "data": {
                "incident_report": {
                    "uri": incident_report_uri,
                    "properties": {
                        "hasSeverityFromCRCL": severity_value
                    }
                }
            }
        }

        self.add_abox_data(json.dumps(insert_query))

    def update_incident_report_severity_calculated_by_crcl(self, report_id, new_severity_value):
        new_severity_value = new_severity_value.lower()
        if new_severity_value not in self._severity_values:
            print("Not a valid severity value from to crcl to update: "+str(new_severity_value))
            return

        try:
            incident_report_uri = self.get_incident_report_uri(report_id)

        except Exception as e:
            print("Error 1 @ WebGenesisClient.set_incident_report_severity_calculated_by_crcl")
            print(e)
            return

        old_severity_value = self.get_incident_report_severity_calculated_by_crcl(report_id)

        if new_severity_value != old_severity_value:
            try:
                self.remove_severity_calculated_by_crcl(incident_report_uri, values=[old_severity_value])
            except:
                print("PROBLEM WITH REMOVING THE OLD CRCL SEVERITY")
                print(incident_report_uri)
                print("old severity:"+str(old_severity_value) + " new severity:" + str(new_severity_value))
                pass

            # Insert severity value
            insert_query = {
                "defaultprefix": "http://beaware-project.eu/beAWARE/#",
                "data": {
                    "incident_report": {
                        "uri": incident_report_uri,
                        "properties": {
                            "hasSeverityFromCRCL": new_severity_value
                        }
                    }
                }
            }

            self.add_abox_data(json.dumps(insert_query))

        print("CRCL UPDATE WAS MADE " + str(old_severity_value) + " --> " + str(new_severity_value))

    def remove_severity_calculated_by_crcl(self, incident_report_uri, values=None):
        values_to_remove = self._severity_values.copy()
        if values is not None:
            values_to_remove = values
            print("Removing old crcl severity values:"+str(values_to_remove))

        for value in values_to_remove:
            # delete severity value
            delete_query = {
                "defaultprefix": "http://beaware-project.eu/beAWARE/#",
                "data": {
                    "individuals": [],
                    "properties": {
                        incident_report_uri: {
                            "hasSeverityFromCRCL": value
                        }
                    }
                }
            }
            self.remove_abox_data(json.dumps(delete_query))

    def get_involved_participants_of_incident(self, incident_uri):
        # print("Detecting participants from uri: " + str(incident_uri))

        query = """
                SELECT ?participant_type ?confidence ?risk ?role ?number ?label
                        (group_concat(?reference_code;separator="|") as ?reference_codes)
                WHERE {
                    {
                        ?participant baw:participantIsInvolvedIn <%s> .
                    }
                    UNION
                    {
                        <%s> baw:involvesParticipant ?participant .
                    }                    
                    
                    ?participant rdf:type ?participant_type_class .                    
                    MINUS {
                        ?participant rdf:type ?super_type .
                        ?super_type rdfs:subClassOf ?participant_type_class .
                        FILTER(?super_type != ?participant_type_class)
                    } 
                    ?participant_type_class rdfs:label ?participant_type .
                    
                    
                    OPTIONAL{
                        ?participant baw:participantIsDetectedBy ?detection .
                        ?detection baw:hasDetectionConfidence ?confidence_numeric .
                        ?detection baw:hasDetectionRisk ?risk_numeric .
    
                        BIND((IF(?confidence_numeric < 0.5, "low", IF (?confidence_numeric < 0.75, "medium", "high"))) AS ?confidence_value) .
                        BIND((IF(?risk_numeric < 0.5, "low", IF (?risk_numeric < 0.75, "medium", "high"))) AS ?risk_value) .
                    } .
                    
                    BIND(IF(BOUND(?confidence_value), ?confidence_value, "undefined") AS ?confidence) .
                    BIND(IF(BOUND(?risk_value), ?risk_value, "undefined") AS ?risk) .
                    
                    OPTIONAL {
                        ?participant baw:hasTextAnalysisRole ?role
                    }.
                    
                    OPTIONAL {
                        ?participant baw:hasTextAnalysisQuantity ?number
                    }.
                    
                    OPTIONAL {
                        ?participant baw:instanceDisplayName ?label
                    }.
                    
                    OPTIONAL {
                        ?participant baw:hasReferenceCode ?reference_code
                    }.

                    MINUS {
                        ?other_class rdfs:subClassOf ?type
                        FILTER (?other_class != ?type)
                    }
                } GROUP BY ?participant ?participant_type ?confidence ?risk ?role ?number ?label
                    ORDER BY ?participant_type
                """ % (incident_uri, incident_uri,)
        try:
            # return self.execute_sparql_select(query)['results']['bindings'][0]['attachment']['value']
            results = self.execute_sparql_select(query)

            incident_list = []

            for result in results['results']['bindings']:
                participant_dict = {
                    "type": result['participant_type']['value'],
                    "confidence": result["confidence"]["value"],
                    "risk": result["risk"]["value"],
                    "role": None,
                    "number": None,
                    "label": None
                }

                # If role was found
                if 'role' in result:
                    participant_dict['role'] = result['role']['value']

                # If number was found
                if 'number' in result:
                    participant_dict['number'] = result['number']['value']

                # If label was found
                if 'label' in result:
                    participant_dict['label'] = result['label']['value']

                # If references were found
                if 'reference_codes' in result and result["reference_codes"]["value"]:
                    participant_dict["refs"] = result["reference_codes"]["value"].split("|")
                else:
                    participant_dict["refs"] = []

                incident_list.append(participant_dict)

            return incident_list

        except Exception as e:
            print("Error - involved participants could not be retrieved.")
            print(e)
            return []

    def get_type_of_incident(self, incident_uri):
        query = """
                SELECT ?type_label 
                WHERE {
                    {
                        <%s> baw:isOfIncidentType ?type.
                    }
                    UNION
                    {
                        ?type baw:hasEffectOccurrence <%s>
                    }
                    
                    ?type rdfs:label ?type_label
                }
                """ % (incident_uri, incident_uri,)

        try:
            results = self.execute_sparql_select(query)
            return results['results']['bindings'][0]['type_label']['value']

        except Exception as e:
            print("Error @ WebGenesisClient.get_type_of_incident()")
            print(e)
            return "undefined"

    def get_attachments_of_incident_report(self, incident_id):
        query = """
                    SELECT ?attachment
                    WHERE {
                        ?incident_report baw:hasReportID "%s" .
                        {                            
                            ?incident_report baw:hasAttachment ?attachment .
                        }
                        UNION
                        {
                            ?attachment baw:isAttachmentOf ?incident_report .
                        }
                    }
        """ % (incident_id,)

        try:
            # return self.execute_sparql_select(query)['results']['bindings'][0]['attachment']['value']
            results = self.execute_sparql_select(query)
            return [result['attachment']['value'] for result in results['results']['bindings']]
        except:
            return None

    def get_description_items_of_incident_report(self, incident_id):
        query = """
                    SELECT ?description
                    WHERE {
                        ?incident_report baw:hasReportID "%s" .
                        {
                            ?incident_report baw:hasDescription ?description .
                        }
                        UNION
                        {
                            ?description baw:isDescriptionOf ?incident_report .
                        }
                    }
        """ % (incident_id,)

        try:
            results = self.execute_sparql_select(query)
            return [result['description']['value'] for result in results['results']['bindings']]
        except:
            return None

    def get_subject(self, predicate, object):
        query = """
            SELECT ?subject
            WHERE {
                ?subject %s %s
            }
        """ % (predicate, object)

        try:
            return self.execute_sparql_select(query)['results']['bindings'][0]['subject']['value']
        except:
            return None

    def get_object(self, subject, predicate):
        query = """
            SELECT ?object
            WHERE {
                %s %s ?object
            }
        """ % (subject, predicate)

        try:
            return self.execute_sparql_select(query)['results']['bindings'][0]['object']['value']
        except:
            return None

    def get_psap_incident_locations(self):
        query = """
                SELECT ?psap_id ?lat ?long
                WHERE {
                    ?incident_report baw:hasReportID ?report_id .
                    ?incident_report baw:hasPSAPIncidentID ?psap_id .
                    ?incident_report baw:hasReportLocation ?location .
                    ?location baw:latitude ?lat .
                    ?location baw:longitude ?long .
                    
                    FILTER(?report_id = ?psap_id)
                }
            """

        try:
            results = self.execute_sparql_select(query)
            # valid_clusters = []
            # # print("Looking for valid clusters"+str(results['results']['bindings']))
            # for cluster in results['results']['bindings']:
            #     inc_category = self.get_incident_raw_category(cluster['psap_id']['value'])
            #     print("category of cluster:" + str(inc_category)+" id: "+ str(cluster['psap_id']['value']))
            #     if inc_category.lower() != "evacuation":  # Exclude evacuation clusters
            #         valid_clusters.append({
            #             "psap_id": cluster['psap_id']['value'],
            #             "lat": cluster["lat"]["value"],
            #             "long": cluster["long"]["value"]
            #         })
            # print("Final clusters:"+str(valid_clusters))
            # return valid_clusters
            return [{
                "psap_id": result['psap_id']['value'],
                "lat": result["lat"]["value"],
                "long": result["long"]["value"]
            } for result in results['results']['bindings']]
        except:
            return []

    def get_incidents_details_from_psap_incident_cluster(self, psap_id, incident_id=None):
        query = """
                SELECT ?report_id ?incident ?timestamp ?attachment_type ?priority ?severity
                        (group_concat(?reference_code;separator="|") as ?reference_codes)
                WHERE {
                    ?incident_report baw:hasPSAPIncidentID '%s' .
                    ?incident_report baw:hasReportID ?report_id .
                    
                    {
                        ?incident_report baw:hasAttachment ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:isAttachmentOf ?incident_report .
                    }
                    UNION
                    {
                        ?incident_report baw:hasDescription ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:isDescriptionOf ?incident_report .
                    }
                    
                    
                    ?attachment baw:hasMediaItemTimestamp ?timestamp .
                    ?attachment rdf:type ?attachment_class .

                    BIND (IF(?attachment_class=baw:ImageItem, "image", IF(?attachment_class=baw:VideoItem, "video", IF(?attachment_class=baw:HTMLItem, "html", IF(?attachment_class=baw:TextItem, "text", IF(?attachment_class=baw:AudioItem, "audio", "other"))))) as ?attachment_type)
                    FILTER(?attachment_class != baw:MediaItem && ?attachment_class != baw:ParameterValue) .

                    { 
                        ?task baw:taskProducesDataset ?dataset .
                    }
                    UNION
                    {
                        ?dataset baw:isProducedByTask ?task .
                    }
                    
                    {
                        ?task baw:relatesToMediaItem ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:relatesToTask ?task .
                    }

                    {
                        ?dataset baw:detectsDatasetIncident ?incident .
                    }
                    UNION
                    {
                        ?incident baw:isDatasetDetectionOf ?dataset .
                    }
                    UNION
                    {
                        ?detection baw:isDetectionOf ?dataset .
                        ?detection baw:detectsIncident ?incident .
                    }
                    UNION  # XXXX
                    {
                        ?dataset baw:containsDetection ?detection .
                        ?detection baw:detectsIncident ?incident .
                    } .

                    OPTIONAL {
                        ?incident baw:hasIncidentPriority ?priority_value .
                        ?incident baw:hasIncidentSeverity ?severity_value .
                    } .
                    
                    OPTIONAL {
                        ?incident baw:hasReferenceCode ?reference_code .
                    } .

                    BIND(IF(BOUND(?priority_value), ?priority_value, "unknown") AS ?priority) .
                    BIND(IF(BOUND(?severity_value), ?severity_value, "unknown") AS ?severity) .
                    
                } GROUP BY ?report_id ?incident ?timestamp ?attachment_type ?priority ?severity
            """ % (psap_id,)

        try:
            results = self.execute_sparql_select(query)
            # print("Results on incidents : "+str(json.dumps(results, indent=2)))
            # print(*[(r["report_id"]["value"], r["report_id"]["value"]) for r in results["results"]["bindings"]], sep="\n")

            incidents_list = []
            for result in results['results']['bindings']:

                incident = {
                    "cluster_id": psap_id,
                    "report_id": result['report_id']['value'],
                    "incident_uri": result['incident']['value'],
                    "timestamp": result['timestamp']['value'],
                    "attachment_type": result['attachment_type']['value'],
                    "priority": result['priority']['value'],
                    "severity": result['severity']['value']
                }

                # Check if references where found
                if 'reference_codes' in result and result['reference_codes']['value']:
                    reference_codes = result['reference_codes']['value'].split("|")

                    incident["refs"] = reference_codes
                else:
                    incident["refs"] = []

                incidents_list.append(incident)
            # print("Number of incidents in Incident list: " + str(len(incidents_list)))
            return incidents_list
        except Exception as e:
            print(e)
            return []

    def get_incidents_details_from_all_psap_incident_clusters(self):
        query = """
                SELECT ?psap_id ?report_id ?incident ?timestamp ?attachment_type ?priority ?severity
                        (group_concat(?reference_code;separator="|") as ?reference_codes)
                WHERE {
                    ?incident_report baw:hasPSAPIncidentID ?psap_id .
                    ?incident_report baw:hasReportID ?report_id .

                    {
                        ?incident_report baw:hasAttachment ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:isAttachmentOf ?incident_report .
                    }
                    UNION
                    {
                        ?incident_report baw:hasDescription ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:isDescriptionOf ?incident_report .
                    }

                    ?attachment baw:hasMediaItemTimestamp ?timestamp .
                    ?attachment rdf:type ?attachment_class .

                    BIND (IF(?attachment_class=baw:ImageItem, "image", IF(?attachment_class=baw:VideoItem, "video", IF(?attachment_class=baw:HTMLItem, "html", IF(?attachment_class=baw:TextItem, "text", IF(?attachment_class=baw:AudioItem, "audio", "other"))))) as ?attachment_type)
                    FILTER(?attachment_class != baw:MediaItem && ?attachment_class != baw:ParameterValue) .

                    { 
                        ?task baw:taskProducesDataset ?dataset .
                    }
                    UNION
                    {
                        ?dataset baw:isProducedByTask ?task .
                    }
                    
                    {
                        ?task baw:relatesToMediaItem ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:relatesToTask ?task .
                    }

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

                    OPTIONAL {
                        ?incident baw:hasIncidentPriority ?priority_value .
                        ?incident baw:hasIncidentSeverity ?severity_value .
                    } .
                    
                    OPTIONAL {
                        ?incident baw:hasReferenceCode ?reference_code .
                    } .

                    BIND(IF(BOUND(?priority_value), ?priority_value, "unknown") AS ?priority) .
                    BIND(IF(BOUND(?severity_value), ?severity_value, "unknown") AS ?severity) .

                } GROUP BY ?psap_id ?report_id ?incident ?timestamp ?attachment_type ?priority ?severity
            """

        try:
            results = self.execute_sparql_select(query)

            incidents_list = []
            for result in results['results']['bindings']:

                incident = {
                    "cluster_id": result['psap_id']['value'],
                    "report_id": result['report_id']['value'],
                    "incident_uri": result['incident']['value'],
                    "timestamp": result['timestamp']['value'],
                    "attachment_type": result['attachment_type']['value'],
                    "priority": result['priority']['value'],
                    "severity": result['severity']['value']
                }

                # Check if references where found
                if 'reference_codes' in result and result['reference_codes']['value']:
                    reference_codes = result['reference_codes']['value'].split("|")

                    incident["refs"] = reference_codes
                else:
                    incident["refs"] = []

                incidents_list.append(incident)

            return incidents_list

        except Exception as e:
            print(e)
            return []

    def get_location_of_incident_report(self, report_id):
        query = """
                SELECT ?lat ?long
                WHERE {
                    ?incident_report baw:hasReportID '%s' .
                    ?location baw:latitude ?lat .
                    ?location baw:longitude ?long .
                    
                    {
                        ?location baw:isLocationOfReport ?incident_report .
                    }
                    UNION
                    {
                        ?incident_report baw:hasReportLocation ?location .
                    }
                    UNION
                    {
                        {
                            ?incident_report baw:hasAttachment ?attachment .
                        }
                        UNION
                        {
                            ?attachment baw:isAttachmentOf ?incident_report .
                        }
                        
                        { 
                            ?task baw:taskProducesDataset ?dataset .
                        }
                        UNION
                        {
                            ?dataset baw:isProducedByTask ?task .
                        }
                        
                        {
                            ?task baw:relatesToMediaItem ?attachment .
                        }
                        UNION
                        {
                            ?attachment baw:relatesToTask ?task .
                        }
    
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
                        
                        ?incident baw:hasIncidentLocation ?location .
                    }
                    
                }
            """ % (report_id,)

        try:
            results = self.execute_sparql_select(query)

            locations = []
            for result in results['results']['bindings']:
                if result["lat"]["value"] != "undefined" and result["long"]["value"] != "undefined":
                    locations.append(
                        {
                            "lat": float(result["lat"]["value"]),
                            "long": float(result["long"]["value"])
                        }
                    )

            return locations[0]

        except Exception as e:
            print("Error @ WebGenesisClient.get_location_of_incident_report()")
            print(e)

            return {
                "lat": "undefined",
                "long": "undefined"
            }

    def get_relief_places(self):
        query = """
                SELECT ?relief_place ?display_name ?lat ?long
                WHERE {
                   ?relief_place rdf:type baw:PlaceOfRelief .
                   ?relief_place baw:instanceDisplayName ?display_name .
                   ?relief_place baw:placeOfReliefLocated ?location .
                   ?location baw:latitude ?lat .
                   ?location baw:longitude ?long .
                }
                """

        try:
            results = self.execute_sparql_select(query)

            return [{
                "relief_place": result["relief_place"]["value"],
                "display_name": result["display_name"]["value"],
                "lat": float(result["lat"]["value"]),
                "long": float(result["long"]["value"])
            } for result in results['results']['bindings']]
        except Exception as e:
            print("Error @ WebGenesisClient.get_relief_places()")
            print(e)
            return None

    def get_location_of_relief_place(self, relief_place_uri):
        query = """
                SELECT ?location
                WHERE {
                   <%s> baw:placeOfReliefLocated ?location .
                }
                """ % (relief_place_uri,)

        try:
            results = self.execute_sparql_select(query)

            return [result["location"]["value"] for result in results['results']['bindings']][0]
        except Exception as e:
            print("Error @ WebGenesisClient.get_location_of_relief_place()")
            print(e)
            return None

    def get_place_of_relief_uri_from_psap_id(self, place_of_relief_incident_report_psap_id):
        query = """
                SELECT ?place_of_relief
                WHERE{
                    ?place_of_relief baw:placeOfReliefLocated ?location .

                    ?place_of_relief_incident_report baw:hasPSAPIncidentID "%s" .
                    ?place_of_relief_incident_report baw:hasReportLocation ?location .
                }
                """ % (place_of_relief_incident_report_psap_id,)

        try:
            results = self.execute_sparql_select(query)
            return results['results']['bindings'][0]['place_of_relief']['value']

        except Exception as e:
            return None

    def get_place_of_relief_capacity(self, place_of_relief_uri):
        return self.get_object("<" + place_of_relief_uri + ">", "baw:capacity")

    def get_place_of_relief_capacity_used(self, place_of_relief_uri):
        return self.get_object("<" + place_of_relief_uri + ">", "baw:capacityUsed")

    def update_place_of_relief_capacity_used(self, place_of_relief_uri, new_value):

        try:
            # Delete old value
            delete_query = {
                "defaultprefix": "http://beaware-project.eu/beAWARE/#",
                "data": {
                    "individuals": [],
                    "properties": {
                        place_of_relief_uri: {
                            "capacityUsed": self.get_place_of_relief_capacity_used(place_of_relief_uri)
                        }
                    }
                }
            }

            self.remove_abox_data(json.dumps(delete_query))
        except:
            pass

        # Insert new value
        insert_query = {
            "defaultprefix": "http://beaware-project.eu/beAWARE/#",
            "data": {
                "place_of_relief_uri": {
                    "uri": place_of_relief_uri,
                    "properties": {
                        "capacityUsed": new_value
                    }
                }
            }
        }

        self.add_abox_data(json.dumps(insert_query))

    def get_incident_type_labels(self):
        query = """
                SELECT ?label
                WHERE {
                   ?incident_type rdf:type baw:IncidentType .
                   ?incident_type rdfs:label ?label
                }
                """

        try:
            results = self.execute_sparql_select(query)
            print("Labels:"+ str([result["label"]["value"] for result in results['results']['bindings']]))
            return [result["label"]["value"] for result in results['results']['bindings']]

        except Exception as e:
            print("Error @ WebGenesisClient.get_incident_type_labels()")
            print(e)
            return []

    def get_incident_category(self, psap_id):
        incident_types = []

        try:
            incidents_of_cluster = self.get_incidents_details_from_psap_incident_cluster(psap_id)

            for incident in incidents_of_cluster:
                incident_types.append(self.get_type_of_incident(incident_uri=incident["incident_uri"]))
            # print("incidents types:" + str(incident_types))
            incident_types = list(filter(lambda x: x != "Evacuation" and x != "Other" and x != "OtherIncident", incident_types))
            print("incidents types without evac/other:" + str(incident_types))
        except Exception as e:
            print("Error @ WebGenesisClient.get_incident_category()")
            print(e)
            return 'Other'

        if incident_types:
            most_common_type = max(set(incident_types), key=incident_types.count)

            if most_common_type == 'Flood' or most_common_type == 'Overflow' or most_common_type == 'Precipitation':
                return 'Met'
            elif most_common_type == 'Fire' or most_common_type == 'Smoke':
                return 'Fire'
            elif most_common_type == 'Traffic':
                return 'Transport'
            elif most_common_type == 'Collapse':
                return 'Infra'
            else:
                return 'Other'
        else:
            return 'Other'

    def get_incident_raw_category(self, psap_id):
        incident_types = []

        try:
            incidents_of_cluster = self.get_incidents_details_from_psap_incident_cluster(psap_id)

            for incident in incidents_of_cluster:
                incident_types.append(self.get_type_of_incident(incident_uri=incident["incident_uri"]))
            # print("Raw incidents types:" + str(incident_types))
            # incident_types = list(filter(lambda x: x != "Evacuation" and x != "Other", incident_types))
        except Exception as e:
            print("Error @ WebGenesisClient.get_incident_category()")
            print(e)
            return 'Other'

        if incident_types:
            most_common_type = max(set(incident_types), key=incident_types.count)

            return most_common_type
        else:
            return 'Other'

    def get_spam_flag(self, report_id):
        query = """
                SELECT ?spam
                WHERE {
                   ?incident_report baw:hasReportID "%s" .
                   ?incident_report baw:isSpam ?spam
                }
                """ % (report_id,)

        try:
            results = self.execute_sparql_select(query)

            if results['results']['bindings']:
                if results['results']['bindings'][0]["spam"]["value"] == "true":
                    return True
                elif results['results']['bindings'][0]["spam"]["value"] == "false":
                    return False
                else:
                    return None
            else:
                return None

        except Exception as e:
            print("Error @ WebGenesisClient.get_spam_flag()")
            print(e)
            return None

    def set_spam_flag(self, report_id, spam_value=False):
        incident_report_uri = self.get_incident_report_uri(report_id)

        # Insert PSAP incident id
        insert_query = {
            "defaultprefix": "http://beaware-project.eu/beAWARE/#",
            "data": {
                "incident": {
                    "uri": incident_report_uri,
                    "properties": {
                        "isSpam": spam_value
                    }
                }
            }
        }

        self.add_abox_data(json.dumps(insert_query))

    def utc_now(self):
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def testing():
    # Prepare configuration for webgenesis
    # with open("webgenesis_credentials.json", "r") as f:
    #     webgenesis_configuration = json.load(f)

    webgenesis_configuration = load_credentials.LoadCredentials.load_wg_credentials()

    # Create webgenesis client
    c = WebGenesisClient(webgenesis_configuration)
    print(webgenesis_configuration)

    query1 = """
    SELECT ?psap_id ?report ?p ?o
    WHERE{
    ?report baw:hasPSAPIncidentID ?psap_id.
    #?report rdf:type ?type.
    ?report ?p ?o.
    }
    """
    query2 = """
    SELECT ?s ?p ?d
    WHERE{
    ?s baw:hasIncidentSeverity ?o. 
    ?s ?p ?d
    }
    """

    query3 = """SELECT ?incident_report ?p ?o
    WHERE
    {
    ?incident_report ?x 'INC_SCAPP_6'.
    ?incident_report ?p ?o. 
    }
    """

    query4 = """
    SELECT ?subject ?predicate ?incident_report
    WHERE{
        ?incident_report ?some_id 'INC_SCAPP_6' . 
        # ?incident_report ?predicate ?object.
        ?subject ?predicate ?incident_report. 
    }
    """

    query = """
                SELECT ?incident_report ?psap_id
                WHERE {
                    ?incident_report baw:hasPSAPIncidentID ?psap_id .
                }
            """

    query_019 = """
                    SELECT ?incident_report ?prop ?obj
                    WHERE {
                        ?incident_report baw:hasPSAPIncidentID 'INC_UAVP_@sinst-id-12ef3240-ccba-11e9-a234-615883b44fb6'.
                        ?incident_report ?prop ?obj .
                    }
                """

    full_query = """
    SELECT ?severity
                WHERE {
                    ?incident_report baw:hasReportID 'INC_UAVP_@sinst-id-12ef3240-ccba-11e9-a234-615883b44fb6' .

                    {
                        ?incident_report baw:hasAttachment ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:isAttachmentOf ?incident_report .
                    }
                    UNION
                    {
                        ?incident_report baw:hasDescription ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:isDescriptionOf ?incident_report .
                    }

                    { 
                        ?task baw:taskProducesDataset ?dataset .
                    }
                    UNION
                    {
                        ?dataset baw:isProducedByTask ?task .
                    }
                    
                    {
                        ?task baw:relatesToMediaItem ?attachment .
                    }
                    UNION
                    {
                        ?attachment baw:relatesToTask ?task .
                    }

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

                    OPTIONAL {
                        ?incident baw:hasIncidentSeverity ?severity_value .
                    } .

                    BIND(IF(BOUND(?severity_value), ?severity_value, "unknown") AS ?severity) .

                }
                """
    query_location = """
                    SELECT ?lat ?long
                    WHERE {
                        ?incident_report baw:hasReportID '14758b7c-e401-407c-8a9d-4504e00d2a89' .
                        ?location baw:latitude ?lat .
                        ?location baw:longitude ?long .

                        {
                            ?location baw:isLocationOfReport ?incident_report .
                        }
                        UNION
                        {
                            ?incident_report baw:hasReportLocation ?location .
                        }
                        UNION
                        {
                            {
                                ?incident_report baw:hasAttachment ?attachment .
                            }
                            UNION
                            {
                                ?attachment baw:isAttachmentOf ?incident_report .
                            }

                            { 
                                ?task baw:taskProducesDataset ?dataset .
                            }
                            UNION
                            {
                                ?dataset baw:isProducedByTask ?task .
                            }

                            {
                                ?task baw:relatesToMediaItem ?attachment .
                            }
                            UNION
                            {
                                ?attachment baw:relatesToTask ?task .
                            }

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

                            ?incident baw:hasIncidentLocation ?location .
                        }

                    }
                """
    query_psap = """
                    SELECT ?psap_id 
                    WHERE {
                        ?incident_report baw:hasReportID 'INC_UAVP_@sinst-id-12ef3240-ccba-11e9-a234-615883b44fb6' .
                        ?incident_report baw:hasPSAPIncidentID ?psap_id .
                    }
            """
    query_018 = """
                        SELECT ?incident_report ?prop ?obj 
                        WHERE {
                            ?incident_report baw:hasReportID 'INC_SCAPP_02134b2d12d64c7286375f24df40f4ff' .
                            ?incident_report ?prop ?obj.
                        }
                """

    query_rule1 = """
                    SELECT DISTINCT ?incident
                    WHERE {
                        ?incident rdf:type baw:Incident .
                        MINUS {?incident baw:isOfIncidentType baw:OtherIncident .}

                        ?participant baw:participantIsInvolvedIn ?incident .
                        ?participant rdf:type baw:Human .

                        MINUS {
                            ?incident baw:hasIncidentSeverity "severe" .
                        }
                    }
                    """
    query_incident_location = """
    SELECT ?incident ?inc_location
                    WHERE {
                        ?incident rdf:type baw:Incident.
                        OPTIONAL {?incident baw:hasIncidentLocation ?inc_location}
                    }
    """

    print(json.dumps(c.execute_sparql_select(query=query_incident_location), indent=2))
    # QueryLogger.flush_entries()
    exit()

    entry = {"label": "add_abox_data", "time_start": 1568014840.943328, "query": {"data": {
        "location_8ecc4cb9-d2a4-47b9-ab53-dc955b26bfad": {"type": "Location",
                                                          "properties": {"latitude": 39.36415, "longitude": -0.37133},
                                                          "preferredUri": "http://beaware-project.eu/beAWARE/#Location_bc550098-0db9-4bf4-a235-d477f6ffd576"},
        "incident_report_8ecc4cb9-d2a4-47b9-ab53-dc955b26bfad": {"type": "IncidentReport", "properties": {
            "hasReportID": "8ecc4cb9-d2a4-47b9-ab53-dc955b26bfad",
            "hasReportLocation": "location_8ecc4cb9-d2a4-47b9-ab53-dc955b26bfad",
            "hasReportText": "{\n   \"header\": {\n      \"topicName\": \"TOP021_INCIDENT_REPORT\",\n      \"topicMajorVersion\": 0,\n      \"topicMinorVersion\": 3,\n      \"sender\": \"SCAPP\",\n      \"msgIdentifier\": \"b4605237-6d50-4670-ae00-823f576d112c_KBS_logs_10\",\n      \"sentUTC\": \"2019-03-07T12:30:44Z\",\n      \"status\": \"Actual\",\n      \"actionType\": \"Alert\",\n      \"specificSender\": \"mobileAppTechnicalUser\",\n      \"scope\": \"Restricted\",\n      \"district\": \"Valencia\",\n      \"recipients\": \"\",\n      \"code\": 0,\n      \"note\": \"\",\n      \"references\": \"\"\n   },\n   \"body\": {\n      \"incidentOriginator\": \"SCAPP\",\n      \"incidentID\": \"8ecc4cb9-d2a4-47b9-ab53-dc955b26bfad\",\n      \"language\": \"es-ES\",\n      \"startTimeUTC\": \"2019-09-09T07:38:31Z\",\n      \"title\": \"Report on valencia\",\n      \"position\": {\n         \"latitude\": 39.36415,\n         \"longitude\": -0.37133\n      },\n      \"description\": \"Lluvias fuertes\"\n   }\n}",
            "incidentReportTimeStamp": "2019-09-09T07:38:31Z", "hasOriginator": "SCAPP",
            "hasDescription": "description_8ecc4cb9-d2a4-47b9-ab53-dc955b26bfad"},
                                                                 "preferredUri": "http://beaware-project.eu/beAWARE/#IncidentReport_799d81cc-d462-4d44-b824-6e6fefa6599c"},
        "description_8ecc4cb9-d2a4-47b9-ab53-dc955b26bfad": {"type": "TextItem",
                                                             "properties": {"hasRawMediaSource": "Lluvias fuertes",
                                                                            "hasMediaItemTimestamp": "2019-09-09T07:38:31Z",
                                                                            "hasMediaItemName": "Description from incident report 8ecc4cb9-d2a4-47b9-ab53-dc955b26bfad"},
                                                             "preferredUri": "http://beaware-project.eu/beAWARE/#TextItem_d4430c67-a7c2-4e05-a132-364f2d0dd2a2"}},
        "defaultprefix": "http://beaware-project.eu/beAWARE/#"},
             "time_end": 1568014841.5304198, "reply_json": {"status_code": 500,
                                                            "text": "{\"error\":{\"code\":\"500\",\"description\":\"An internal error occured.\\u003cbr /\\u003eThe details were sent to the administrator of the server.\"}}",
                                                            "url": "https://beaware-1.eu-de.containers.appdomain.cloud/servlet/is/rest/entry/1932/addABoxData/"}}
    query = entry['query']
    query = json.dumps(query)
    print(query)
    print(c.add_abox_data(json_query=query))
    # print(c.execute_sparql_select(
    #     """SELECT ?label WHERE {<http://beaware-project.eu/beAWARE/#Human> rdfs:label ?label} LIMIT 10"""))
