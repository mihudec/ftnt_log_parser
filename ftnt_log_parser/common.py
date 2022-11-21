from audioop import mul
from email.generator import Generator
from itertools import zip_longest
import re
import itertools
import datetime
import gzip
import tarfile
import pathlib
import shlex
import json
from typing import Iterable, Union, Literal, Generator, Dict
import pandas as pd

from ftnt_log_parser.config import CONFIG
from ftnt_log_parser.utils import dict_update_path


LOG_KEY_PATTERN = re.compile(pattern=r"(?:^| )(?P<key>[a-z_]+)=", flags=re.MULTILINE)

def pairwise(data):
    it = iter(data)
    return zip(it,it)

class LogLoader:
    def __init__(self) -> None:
        self.config = CONFIG

    @staticmethod
    def read_plaintext(file: pathlib.Path) -> Generator[str, None, None]:
        with file.open(mode='r', encoding=CONFIG.ENCODING) as f:
            for line in f.readlines():
                yield line.strip()

    @staticmethod
    def read_gzip(file: pathlib.Path) -> Generator[str, None, None]:
        with gzip.open(file) as f:
            for line in f:
                yield line.decode(encoding='utf-8').strip()

    @staticmethod
    def read_tar(file: pathlib.Path) -> Generator[str, None, None]:
        tar = tarfile.open(file, "r:gz")
        for member in tar.getmembers():
            f = tar.extractfile(member)
            if f is not None:
                for line in f:
                    yield line.decode(encoding='utf-8').strip()

    @staticmethod
    def determine_filetype(file: pathlib.Path) -> Literal['plain', 'gz', 'tgz']:
        if file.suffix in ['.txt', '.log']:
            return 'plain'
        elif file.suffix in ['.gz']:
            return 'gz'
        elif file.suffix in ['.tgz']:
            return 'tgz'
        else:
            return None
    
    @staticmethod
    def read_lines(file: pathlib.Path, compression_type: Literal['plain', 'gz', 'tgz', None] = None) -> Generator[str, None, None]:
        if compression_type is None:
            compression_type = LogLoader.determine_filetype(file=file)
        
        if compression_type == 'plain':
            return LogLoader.read_plaintext(file=file)
        elif compression_type == 'gz':
            return LogLoader.read_gzip(file=file)
        elif compression_type == 'tgz':
            return LogLoader.read_tar(file=file)
    
    def get_size(file: pathlib.Path):
        counter = 0
        for line in LogLoader.read_lines(file=file):
            counter += 1
        return counter

    @staticmethod
    def re_parse_lines(lines: Iterable[str]) -> Generator[Dict, None, None]:
        for line in lines:
            kv_list = LOG_KEY_PATTERN.split(string=line)[1:]
            data = {k: v.strip('"') for k, v in pairwise(kv_list)}
            yield data
    
    # TODO: Fix
    @staticmethod
    def shlex_parse_lines(lines: Iterable[str]) -> Generator[Dict, None, None]:
        line_parts = None
        for line in lines:
            try:
                line_parts = shlex.split(line)
            except ValueError as e:
                print(f"ERROR: Line '{line}', Exc: {repr(e)}")
                yield None
            else:
                data = {k: v for k,v in [x.split('=', 1) for x in line_parts]}
                yield data
    
    @staticmethod
    def add_timestamp(entries: Iterable[dict], ts_key: str = '@timestamp') -> Generator[Dict, None, None]:
        timezone = CONFIG.DEFAULT_TIMEZONE
        for entry in entries:
            entry_keys = list(entry.keys())
            timstamp = None
            tzinfo = None
            if 'tz' in entry_keys:
                tzinfo = datetime.datetime.strptime(entry['tz'], '%z').tzinfo
            # else: 
            #     timezone = CONFIG.DEFAULT_TIMEZONE
            timestamp = datetime.datetime.strptime(f"{entry['date']}_{entry['time']}", "%Y-%m-%d_%H:%M:%S")
            # if 'itime' in entry_keys:
            #     timestamp = datetime.datetime.fromtimestamp(int(entry['itime']))
            if tzinfo is not None:
                timestamp = timestamp.replace(tzinfo=tzinfo)
            else:
                timestamp = timezone.localize(timestamp)
            entry[ts_key] = timestamp
            yield entry

    @staticmethod
    def enrich_documents(entries: Iterable[dict], enrich_dict: dict = None):
        if enrich_dict is None:
            return entries
        else:
            for entry in entries:
                entry_keys = list(entry.keys())
                for multikey, value in enrich_dict.items():
                    entry = dict_update_path(orig=entry, path=multikey, value=value)
                yield entry
                        
    
    def head(entries: Iterable, count: int = 10) -> Generator:
        for i in itertools.islice(entries, count):
            yield i

    def format(entries: Iterable[Dict], format: Literal['json', 'json-pretty'] = 'json') -> Generator[str, None, None]:
        for entry in entries:
            if format == 'json':
                yield json.dumps(entry, default=str)

    def file_to_df(file: pathlib.Path):
        lines = LogLoader.read_lines(file=file)
        entries = LogLoader.re_parse_lines(lines=lines)
        entries = LogLoader.add_timestamp(entries=entries)
        df = pd.DataFrame.from_records(data=entries)
        return df

    



def load_gzip(file: pathlib.Path):
    with gzip.open(file) as f:
        for line in f:
            yield line.decode(encoding='utf-8')

def load_tar(file: pathlib.Path):
    tar = tarfile.open(file, "r:gz")
    for member in tar.getmembers():
        f = tar.extractfile(member)
        if f is not None:
            for line in f:
                yield line.decode(encoding='utf-8').strip()

def load_plaintext(file: pathlib.Path):
    with file.open(encoding='utf-8') as f:
        for line in file.read_text().splitlines():
            yield line

def load_file(file: pathlib.Path, compression_type: Union[None, Literal['gz', 'tar']] = None):
    if compression_type is None:
        print(f"Suffix: {file.suffix}")
        if file.suffix in ['.txt', '.log']:
            compression_type = None
        elif file.suffix in ['.gz']:
            compression_type = 'gz'
    if compression_type is None:
        return load_plaintext(file=file)
    elif compression_type == 'gz':
        return load_gzip(file=file)


def re_split_logline(line: str):
    kv_list = LOG_KEY_PATTERN.split(string=line)[1:]
    data = {k: v for k, v in pairwise(kv_list)}
    return data

def add_timestamp(data: dict, ts_key: str = 'itime') -> None:
    timestamp = datetime.datetime.strptime(f"{data['date']}_{data['time']}", "%Y-%m-%d_%H:%M:%S")
    data[ts_key] = timestamp.timestamp()

def enrich_records(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    for k, v in data.items():
        df[k] = v
    return df    

def split_logline(line: str):
    line = line
    try:
        line = shlex.split(line)
    except ValueError as e:
        print(f"ERROR: Line '{line}', Exc: {repr(e)}")
        return None
    else:
        line = {k: v for k,v in [x.split('=', 1) for x in line]}
        return line


def file_to_records(file: pathlib.Path):
    line_iterator = load_gzip(file=file)
    for line in line_iterator:
        line = split_logline(line=line)
        if line is not None:
            yield line

def unifi_df(df: pd.DataFrame):
    df['timestamp'] = pd.to_datetime(df['itime'], unit='s', utc=True).dt.tz_convert('Europe/Prague')
    df.drop(['itime', 'date', 'time'], axis=1, inplace=True)
    df = df = df.where(pd.notnull(df), None)

    return df

def file_to_df(file: pathlib.Path):
    df =  pd.DataFrame.from_records(file_to_records(file=file))
    df = unifi_df(df=df)
    return df

def records_to_df(records: list):
    df = pd.DataFrame.from_records(data=records)
    df = unifi_df(df=df)
    return df
