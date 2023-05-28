import logging
import federator

logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(name)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


if __name__ == "__main__":
    logger.debug("Running main aplication...")
    federator.run()