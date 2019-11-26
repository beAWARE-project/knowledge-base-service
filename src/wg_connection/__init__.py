from webgenesis_client import WebGenesisClient
import load_credentials

webgenesis_configuration = load_credentials.LoadCredentials.load_wg_credentials()
wg_client = WebGenesisClient(webgenesis_configuration)

