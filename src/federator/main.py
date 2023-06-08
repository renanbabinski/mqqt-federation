import logging
import federator
import argparse
from conf import read_config_file

logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(name)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Parse command-line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description='Federator Configuration')
    parser.add_argument('-c', '--config', type=str, help='Path to TOML configuration file')
    return parser.parse_args()


if __name__ == "__main__":
    logger.debug("Running main aplication...")

    args = parse_arguments()

    if args.config:
        config = read_config_file(args.config)

        if config is not None:
            print(config)

    else:
        print("Error: Please provide a TOML configuration file using the -c or --config argument.")

    # federator.run()