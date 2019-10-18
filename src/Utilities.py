from uuid import uuid4


def add_preferredURI(individual_dict):
    try:
        individual_dict["preferredUri"] = "http://beaware-project.eu/beAWARE/#" + str(
            individual_dict["type"]) + "_" + str(uuid4())
        # print("Successfully added preferred Uri")
    except Exception as e:
        print("Error:" + str(e))
        print("Could not add preferred Uri in individual:" + str(individual_dict))


def get_evac_status(message):
    evacuation = None
    try:
        # print(self.message["body"]["analysisTasks"])
        if "Evacuation" in message["body"]["analysisTasks"]:
            evacuation = "inProgress"

        if message["body"]["EvacuationStop"] and message["body"]["EvacuationStop"] is True:
            evacuation = "end"

        return evacuation
    except Exception as e:
        # print("No evacuation field in TOP 019")
        return evacuation


def add_incident_location(data_dict, incident_id, incident_dict_key, msg):
    import json
    import random

    # print("\nDATA BEFORE INCIDENT LOCATION:"+str(json.dumps(data_dict, indent=2)))

    try:
        if "location" in msg["body"]:
            long = msg["body"]["location"]["longitude"]
            lat = msg["body"]["location"]["latitude"]
        elif "position" in msg["body"]:
            long = msg["body"]["position"]["longitude"]
            lat = msg["body"]["position"]["latitude"]
        else:
            raise KeyError
    except Exception as e:
        print("(Utilities.py) Could not add incident location, message has no location: "+str(json.dumps(msg, indent=2)))
        return

    # print(data_dict)

    inc_loc_id = "incident_location_" + str(incident_id) + "_" + str(random.randint(1,1000000))

    data_dict[inc_loc_id] = {
        "type": "Location",
        "properties": {
            "latitude": lat,
            "longitude": long
        }
    }
    add_preferredURI(data_dict[inc_loc_id])

    # print("Before the problem:"+str(data_dict))

    # print("Incident id: " + str(dict_key))

    data_dict[incident_dict_key]["properties"]["hasIncidentLocation"] = inc_loc_id

    # print("\nDATA AFTER INCIDENT LOCATION:"+str(json.dumps(data_dict, indent=2)))