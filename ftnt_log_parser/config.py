from argparse import Namespace
import sys
import pytz
import pathlib
import logging
import json
import yaml

from pydantic import BaseSettings, Field, validator
from pydantic import HttpUrl, FilePath
from pydantic.typing import Any, Dict, Optional, Union


DEFAULT_CONFIG_PATH = pathlib.Path.home().joinpath('.flpconfig.yml')
LOGGER = logging.getLogger(name='FLP')


class FLPConfigBase(BaseSettings):

    class Config:
        validate_assignment = True


    @classmethod
    def from_yaml(cls, text: str):
        data = yaml.safe_load(text)
        config = None
        try:
            config = data.get('config')
        except KeyError as e:
            LOGGER.critical(msg="YAML Config does not contain required key 'config'")
        except AttributeError as e:
            LOGGER.critical(msg="Could not load YAML Config. Please check the config file.")
        except Exception as e:
            LOGGER.critical(msg=f"Unhandled Exception occurred while building TIL Config. {repr(e)}")
        if config is not None:
            config = cls.parse_obj(config)
        return config

    def sdict(self):
        # Return serialized dict
        sdict = json.loads(self.json(exclude_none=True))
        return sdict

    def yaml(self):
        return yaml.safe_dump(data=self.sdict())

    @classmethod
    def from_file(cls, path: pathlib.Path):
        if not isinstance(path, pathlib.Path):
            path = pathlib.Path(path).resolve()
        text = None
        config = None
        if not path.exists():
            LOGGER.error(f"Given path for config does not exist: {path}")
        else:
            text = path.read_text()
        if path.suffix in ['.yml', '.yaml']:
            config = cls.from_yaml(text=text)
        else:
            LOGGER.error(msg=f"Cannot determine config file format based on suffix, got {path.suffix=}")
        return config

class FLPElasticConfig(FLPConfigBase):

    url: str
    username: str
    password: Optional[str]
    ca_cert: Optional[FilePath]
    index: Optional[str]
    pipeline: Optional[str]
    id_key: Optional[str]

class FLPConfig(FLPConfigBase):

    elasticsearch: FLPElasticConfig = Field(FLPElasticConfig(url='http://127.0.0.1:9200', username='elastic'))
    encoding: str = Field('utf-8')
    timezone: Any = Field("UTC")
    enrich: Optional[Dict]

    @validator('timezone',pre=True, allow_reuse=True)
    def validate_timezone(cls, value):
        if isinstance(value, str):
            try: 
                value = pytz.timezone(value)
            except Exception as e:
                raise AssertionError(f"Value {value} is not recognized as valid Timezone")
        return value

    @property
    def ENCODING(self):
        return self.encoding
    
    @property
    def DEFAULT_TIMEZONE(self):
        return self.timezone

class FortinetLogParserConfig:
    
    def __init__(self) -> None:
        self.ENCODING = 'utf-8'
        self.DEFAULT_TIMEZONE = pytz.timezone('Europe/Prague')

def get_config(args: Union[Dict, Namespace] = Namespace()):
    global LOGGER
    if isinstance(args, dict):
        args = Namespace(args)
    config_file_path = getattr(args, 'config_file', None)
    if config_file_path is None:
        config_file_path = DEFAULT_CONFIG_PATH
    print(config_file_path)

    config = None

    if config_file_path.exists():
        LOGGER.debug(msg=f"Settings file {config_file_path} exists, loading_settings.")
        config = FLPConfig.from_file(path=config_file_path)
        if config is None:
            LOGGER.critical("Failed to load settings, exiting.")
            sys.exit(1)
    else:
        LOGGER.debug(msg=f"Settings file {config_file_path} does not exists, using defaults.")
        config = FLPConfig()

    for field_name in FLPConfig.__fields__.keys():
        value = getattr(args, field_name, None)
        if value is not None:
            setattr(config, field_name, value)
    

    return config

# CONFIG = FortinetLogParserConfig()
CONFIG = get_config()