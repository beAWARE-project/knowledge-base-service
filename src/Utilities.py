from uuid import uuid4


def add_preferredURI(individual_dict):
    try:
        individual_dict["preferredUri"] = "http://beaware-project.eu/beAWARE/#" + str(
            individual_dict["type"]) + "_" + str(uuid4())
        print("Successfully added preferred Uri")
    except Exception as e:
        print("Error:" + str(e))
        print("Could not add preferred Uri in individual:" + str(individual_dict))