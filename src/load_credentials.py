import os
import json


class LoadCredentials:
    """
    Loads the webgenesis credentials and bus credentials from environment variables.

    If environment vars are not found then attempt to load from local files.
    """

    # filename of wg entrypoint
    _wg_entrypoint_file = "webgenesis_entrypoint.json"

    # the following are the ENV VARs
    _wg_username_env = "WG_USERNAME"
    _wg_pass_env = "WG_PASSWORD"

    _api_key_env = "SECRET_MH_API_KEY"
    # _kafka_admin_url_env = ""
    _kafka_brokers_sasl_env = "SECRET_MH_BROKERS"

    @staticmethod
    def load_bus_credentials():
        """
        Load the values from environment variables

        :return: dictionary with keys ['api_key', 'kafka_admin_url', 'kafka_brokers_sasl']
        """
        cred = dict()

        try:
            cred['api_key'] = os.environ[LoadCredentials._api_key_env]
            cred['kafka_brokers_sasl'] = os.environ[LoadCredentials._kafka_brokers_sasl_env]
            print("kafka brokers sasl:" + os.environ[LoadCredentials._kafka_brokers_sasl_env])
        except:
            print(
                "Error @ load bus credentials using the keys:" + LoadCredentials._api_key_env + " " + LoadCredentials._kafka_brokers_sasl_env)
            bus_cred_file = "bus_credentials.json"
            print("Trying to find values in file instead:" + bus_cred_file)
            with open(bus_cred_file) as f:
                return json.load(f)

        return cred

    @staticmethod
    def load_wg_credentials():
        """
        Load the password and username for wg from environment variables
        and hostname and ontology_entry_id from local json life ()

        :return:
        """

        cred = dict()
        try:
            cred['username'] = os.environ[LoadCredentials._wg_username_env]
            cred['password'] = os.environ[LoadCredentials._wg_pass_env]
        except:
            print(
                "Error @ load bus credentials using the keys:" + LoadCredentials._wg_username_env + " " + LoadCredentials._wg_pass_env)

            # for the local deployment
            wg_cred_path = "webgenesis_credentials.json"
            print("Trying to find values in file instead:" + wg_cred_path)
            with open(wg_cred_path, 'r') as f:
                return json.load(f)

        with open(LoadCredentials._wg_entrypoint_file, "r") as f:
            wg = json.load(f)
        cred['hostname'] = wg['hostname']
        cred['ontology_entry_id'] = wg['ontology_entry_id']

        return cred


if __name__ == "__main__":
    # LoadCredentials.load_bus_credentials()
    # print("hey")
    os.environ["WG_PASSWORD"] = "XXX_pass"
    os.environ["WG_USERNAME"] = "XXX_username"
    print(LoadCredentials.load_wg_credentials())
    # print(LoadCredentials.load_bus_credentials())
