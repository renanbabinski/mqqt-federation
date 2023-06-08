import toml
from typing import List
from dataclasses import dataclass

@dataclass
class BrokerConfig:
    id: int
    uri: str

@dataclass
class FederatorConfig:
    redundancy: int
    host: BrokerConfig
    neighbors: List[BrokerConfig]


# Read and parse the TOML file
def read_config_file(file_path):
    try:
        with open(file_path, 'r') as file:
            config_data = toml.load(file)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except Exception as e:
        print(f"Error: Failed to parse TOML file '{file_path}': {e}")
        return None

    try:
        # Extract the values from the parsed TOML data
        redundancy = config_data['redundancy']

        host_data = config_data['host']
        host = BrokerConfig(id=host_data['id'], uri=host_data['uri'])

        neighbor_data = config_data['neighbors']
        neighbors = [BrokerConfig(id=n['id'], uri=n['uri']) for n in neighbor_data]

        # Create the FederatorConfig object
        federator_config = FederatorConfig(
            redundancy=redundancy,
            host=host,
            neighbors=neighbors
        )

        return federator_config

    except KeyError as e:
        print(f"Error: Missing key '{e.args[0]}' in TOML file.")
        return None
    except Exception as e:
        print(f"Error: Failed to create configuration object: {e}")
        return None
