from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import HttpResponseError

def get_secret(key:str):
    try:
        # Authenticate to Key Vault
        credential = DefaultAzureCredential()
        kv_client = SecretClient(vault_url="https://nccos-kv-dev.vault.azure.net/", credential=credential)

        # Retrieve secret value
        retrieved_secret = kv_client.get_secret(key)
        secret_value = retrieved_secret.value

        print(f"Retrieved secret '{retrieved_secret.name}' with value: {secret_value}")
        return secret_value
    except HttpResponseError as e:
        print(f"An error occurred when interacting with Key Vault: \n{e}")
        return ""
    except Exception as e:
        print(f"Unexpected error occurred: \n{e}")
        return ""