import sys
import argparse
import pathlib
import itertools
from copy import deepcopy
from elasticsearch import Elasticsearch
from ftnt_log_parser.common import LOG_KEY_PATTERN, LogLoader
from ftnt_log_parser.elasticsearch_indexer import ElasticIndexer
from ftnt_log_parser.config import CONFIG, FLPConfig, get_config

CWD = pathlib.Path.cwd()


def to_path(path_str: str):
    path = None
    path_candidate = pathlib.Path(path_str).resolve()
    if path_candidate.exists():
        path = path_candidate
    else:
        path_candidate = CWD.joinpath(path_str)
        if path_candidate.exists():
            path = path_candidate
        else:
            raise argparse.ArgumentTypeError("Path does not exist")
    return path

class ParseKwargs(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())
        for value in values:
            k, v = value.split('=')
            getattr(namespace, self.dest)[k] = v

class Cli(object):

    def __init__(self) -> None:
        self.CONFIG: FLPConfig = None
        parser = argparse.ArgumentParser(
            description="",
            usage="flp <command> [<args>]"
        )
        parser.add_argument('command', help='Subcommand to run. Options: {read,index}')
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            exit(1)
        getattr(self, args.command)()


    @property
    def _common_parser(self):
        common_parser = argparse.ArgumentParser()
        common_parser.add_argument(
            '-c',
            '--config-file',
            dest='config_file',
            required=False,
            type=to_path

        )
        common_parser.add_argument(
            '-i',
            '--input-file',
            dest='input_files',
            action='append',
            required=True,
            type=to_path

        )
        

        return deepcopy(common_parser)

    def read(self):
        parser = self._common_parser
        parser.description = "Read the logfile and output to stdout"
        parser.usage = "flp read [<args>]"
        parser.add_argument(
            '--head',
            dest='head',
            type=int,
            help="Number of rows to output"
        )
        parser.add_argument(
            '--parse',
            dest='parse',
            action='store_true',
            default=False,
            help="Wether to parse the lines"
        )
        parser.add_argument(
            '--format',
            dest='format',
            choices=['json', 'json-pretty'],
            help="Output format"
        )
        args = parser.parse_args(sys.argv[2:])
        self.CONFIG = get_config(sys.argv)
        input_files = args.input_files
        for input_file in input_files:
            lines = LogLoader.read_lines(file=input_file)
            if args.head is not None:
                lines = LogLoader.head(lines, count=args.head)
            if args.parse is True:
                lines = LogLoader.re_parse_lines(lines=lines)
                if args.format is not None:
                    lines = LogLoader.format(entries=lines, format=args.format)
            for line in lines:
                print(line)
    
    def index(self):
        parser = self._common_parser
        parser.add_argument('--index', dest='elasticsearch_index', required=True)
        parser.add_argument('--pipeline', dest='elasticsearch_pipeline', required=False, help="Name of the Ingest Pipeline")
        parser.add_argument('--id-key', dest='id_key', required=False, help="Key to use for Elastic _id field", default=None)
        parser.add_argument('--head', dest='head', required=False, default=None, type=int, help="Number of HEAD lines to index")
        parser.add_argument('--enrich', dest='enrich', nargs='*', action=ParseKwargs, default=dict())

        parser.description = "Read the logfile and send to Elasticsearch"
        parser.usage = "flp index [<args>]"
        args = parser.parse_args(sys.argv[2:])
        self.CONFIG = get_config(args=sys.argv)
        input_files = args.input_files
        print(self.CONFIG)

        es_client = Elasticsearch(
            hosts=self.CONFIG.elasticsearch.url,
            basic_auth=(self.CONFIG.elasticsearch.username, self.CONFIG.elasticsearch.password),
            ca_certs=self.CONFIG.elasticsearch.ca_cert,
            verify_certs=False,
            ssl_show_warn=False
        )
        id_key = "msg_id"
        if args.id_key is not None:
            id_key = args.id_key
        ei = ElasticIndexer(client=es_client, index_name=args.elasticsearch_index, pipeline=args.elasticsearch_pipeline, id_key=id_key)
        total_records = 0
        for input_file in input_files:
            total_records = LogLoader.get_size(file=input_file)
            print(f"Total records to index: {total_records}")
            lines = LogLoader.read_lines(file=input_file)
            head = args.head
            if head is not None:
                lines = itertools.islice(lines, head)
            entries = LogLoader.re_parse_lines(lines=lines)
            entries = LogLoader.add_timestamp(entries=entries)
            if self.CONFIG.enrich is not None:
                entries = LogLoader.enrich_documents(entries=entries, enrich_dict=self.CONFIG.enrich)
            ei.index_data(data=entries, total_records=total_records)


            

def main():
    Cli()