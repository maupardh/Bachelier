import yaml
import os.path

config_loaded = False
directories = {}

if not config_loaded:
    with open("F:\prod\Bachelier\Configs\directories.yaml", 'r') as f:
        directories = yaml.safe_load(f)
    config_loaded = True

directories['assetsPath'] = os.path.join(directories['bachelierOutputRootDirectory'], directories['assetsPath'])
directories['extradayPricesPath'] = os.path.join(directories['bachelierOutputRootDirectory'], directories['extradayPricesPath'])
directories['intradayPricesPath'] = os.path.join(directories['bachelierOutputRootDirectory'], directories['intradayPricesPath'])
directories['logsPath'] = os.path.join(directories['bachelierOutputRootDirectory'], directories['logsPath'])

