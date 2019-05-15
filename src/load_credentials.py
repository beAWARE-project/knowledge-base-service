import os
import json


class LoadCredentials:
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
        cred['api_key'] = os.environ[LoadCredentials._api_key_env]
        # cred['kafka_admin_url'] = "https://kafka-admin-prod02.messagehub.services.eu-gb.bluemix.net:443"
        cred['kafka_brokers_sasl'] = os.environ[LoadCredentials._kafka_brokers_sasl_env]

        return cred

    @staticmethod
    def load_wg_credentials():
        """
        Load the password and username for wg from environment variables
        and hostname and ontology_entry_id from local json life ()

        :return:
        """

        cred = dict()
        cred['username'] = os.environ[LoadCredentials._wg_username_env]
        cred['password'] = os.environ[LoadCredentials._wg_pass_env]
        with open(LoadCredentials._wg_entrypoint_file, "r") as f:
            wg = json.load(f)
        cred['hostname'] = wg['hostname']
        cred['ontology_entry_id'] = wg['ontology_entry_id']

        return cred


if __name__ == "__main__":
    os.environ["WG_PASSWORD"] = "XXX_pass"
    os.environ["WG_USERNAME"] = "XXX_username"
    print(LoadCredentials.load_wg_credentials())
