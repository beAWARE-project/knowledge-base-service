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
            cred['kafka_brokers_sasl'] = cred['kafka_brokers_sasl'].split(',')
        except:
            # If failed reading the env vars, then try to load the values from the local file (as was previously)
            bus_cred_file = "bus_credentials.json"
            print("Loading values form file instead:" + bus_cred_file)
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
            # If failed reading the env vars, then try to load the values from the local file (as was previously)
            wg_cred_path = "webgenesis_credentials.json"
            print("Loading values form file instead:" + wg_cred_path)
            with open(wg_cred_path, 'r') as f:
                return json.load(f)
        finally:
            with open(LoadCredentials._wg_entrypoint_file, "r") as f:
                wg = json.load(f)
            cred['hostname'] = wg['hostname']
            cred['ontology_entry_id'] = wg['ontology_entry_id']

        return cred


if __name__ == "__main__":
    # os.environ["WG_PASSWORD"] = "XXX_pass"
    # os.environ["WG_USERNAME"] = "XXX_username"
    crebus = LoadCredentials.load_bus_credentials()
    crewg = LoadCredentials.load_wg_credentials()
    print(crewg)
