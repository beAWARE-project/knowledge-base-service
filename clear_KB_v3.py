from webgenesis_client import WebGenesisClient
import json


def get_classes():
    return [
        "Location",
        "IncidentReport",
        "ImageItem",
        "VideoItem",
        "AudioItem",
        "MediaItem",
        "TextItem",
        "HTMLItem",
        "Incident",
        "Detection",
        "DataSet",
        "ImageAnalysis",
        "VideoAnalysis",
        "TextAnalysis",
        "Car",
        "Bicycle",
        "Truck",
        "Bus",
        "Boat",
        "Motorcycle",
        "Human",
    ]


def get_instances_of_class(webgenesis_client, class_name):
    print("Retrieving instances of class ", class_name)

    class_uri = "http://beaware-project.eu/beAWARE/#" + class_name

    query = """
                SELECT ?instance
                WHERE {
                    ?instance rdf:type <%s>
                }
                """ % (class_uri,)

    try:
        instances = webgenesis_client.execute_sparql_select(query)

        print(" >> " + str(len(instances['results']['bindings'])) + " instances found.")

        return [instance['instance']['value'] for instance in instances['results']['bindings']]

    except:
        return []


def get_all_instances(webgenesis_client):
    all_instances = []
    for cls in get_classes():
        all_instances.extend(get_instances_of_class(webgenesis_client, cls))

    return all_instances


def remove_instances(webgenesis_client):
    # Get all instances from KB
    all_instances = get_all_instances(webgenesis_client)

    # These instances should not be removed
    excluded_instances = [
        "http://beaware-project.eu/beAWARE/#location_4th_kapi",
        "http://beaware-project.eu/beAWARE/#location_6th_kapi"
    ]

    instances_to_delete = []

    for instance in all_instances:
        if instance not in excluded_instances:
            instances_to_delete.append(instance)

    print("Instances found", len(all_instances))
    print("Instances to be deleted", len(instances_to_delete))

    # The delete query for all instances that need to be deleted
    delete_query = {
        "defaultprefix": "http://beaware-project.eu/beAWARE/#",
        "data": {
            "individuals": instances_to_delete
        }
    }

    print("Requesting deletion")
    webgenesis_client.remove_abox_data(json.dumps(delete_query))
    print("Deletion complete")


if __name__ == "__main__":
    with open("webgenesis_credentials.json", "r") as f:
        webgenesis_configuration = json.load(f)

    wg = WebGenesisClient(webgenesis_configuration)

    print("Initiating process...")

    # Remove instances from WG
    # print(">> Removing instances from WG")
    # remove_instances(webgenesis_client=wg)

    # Remove report texts from SQLite database
    print(">> Removing report texts from SQLite DB")
    wg.delete_all_incident_report_texts_in_sqlite()
