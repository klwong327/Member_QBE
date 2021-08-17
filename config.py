import yaml
import logging
import logging.config

# todo: use singleton pattern

with open("config.yaml", "r") as f:
    settings = yaml.safe_load(f.read())


with open('logging.yaml', 'r') as f:
  log_cfg = yaml.safe_load(f.read())

logging.config.dictConfig(log_cfg)
logger = logging.getLogger('dev')
logger.setLevel(logging.INFO)
while logger.handlers:
    logger.handlers.pop()