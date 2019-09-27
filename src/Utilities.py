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
    try:
        # print(self.message["body"]["analysisTasks"])
        if "Evacuation" in message["body"]["analysisTasks"]:
            evacuation = "inProgress"

        if message["body"]["EvacuationStop"] and message["body"]["EvacuationStop"] is True:
            evacuation = "end"

        return evacuation
    except Exception as e:
        # print("No evacuation field in TOP 019")
        return None
