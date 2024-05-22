import json
import pathlib
import pandas as pd

from ftnt_log_parser.common import LogLoader

class LogAnalytics:

    def __init__(self) -> None:
        self.BASE_CACHE_DIR = None
        self.DF_CACHE_DIR = None
        self.DF_CACHE_MAP_PATH = None
        self.DF_CACHE_MAP = None

    def _preparation(self):
        self.BASE_CACHE_DIR.mkdir(exist_ok=True)
        self.DF_CACHE_DIR = self.BASE_CACHE_DIR.joinpath("dataframes")
        self.DF_CACHE_DIR.mkdir(exist_ok=True)
        self.DF_CACHE_MAP_PATH = self.DF_CACHE_DIR.joinpath("dataframes.json")
        self.DF_CACHE_MAP_PATH.touch()

    def _load_df_cache_map(self):
        with self.DF_CACHE_MAP_PATH.open() as fp:
            self.DF_CACHE_MAP = json.load(fp=fp)
    
    def _store_df_cache_map(self):
        with self.DF_CACHE_MAP_PATH.open(mode="w") as fp:
            json.dump(obj=self.DF_CACHE_MAP, fp=fp, indent=2)

    def path_to_df(self, path: str, keep_columns: list[str] = None):
        self._load_df_cache_map()
        path = pathlib.Path(path)

        file_name = path.name
        file_base_name = file_name.split('.')[0]

        df = None
        pickle_path = None

        if self.DF_CACHE_MAP.get(file_name, None) is not None:
            pickle_path = self.DF_CACHE_MAP.get(file_name)
            print(f"Loading {file_name} from cache")
            df = pd.read_pickle(pickle_path)
        else:
            pickle_path = self.DF_CACHE_DIR.joinpath(f"{file_base_name}.pkl")
            df = LogLoader.file_to_df(file=path)
            
            if keep_columns is not None:
                df.drop([x for x in df.columns if x not in keep_columns], axis=1, inplace=True)
            
            print(f"Storing {file_name} to cache")
            df.to_pickle(pickle_path)
            self.DF_CACHE_MAP[file_name] = str(pickle_path)
        
        self._store_df_cache_map()

        return df
    
    def remove_na_rows(self, df):
        # Get the number of rows before dropping
        rows_before = df.shape[0]
        # Drop records with NoneType
        df.dropna(inplace=True)
        # Get the number of rows after dropping
        rows_after = df.shape[0]
        # Calculate the number of rows dropped
        rows_dropped = rows_before - rows_after

        print(f"Number of rows dropped: {rows_dropped}")

    def filter_by_timerange(self, df, start_time, end_time, timezone='Europe/Prague'):
        """
        Filter rows in a pandas DataFrame based on a time range defined by start_time and end_time.

        Parameters:
            df (pandas.DataFrame): The DataFrame to filter.
            start_time (str): The start time of the range in ISO format (e.g., '2024-01-01T00:00:00Z').
            end_time (str): The end time of the range in ISO format (e.g., '2024-01-02T00:00:00Z').
            timezone (str): The timezone for start_time and end_time. Default is 'UTC'.

        Returns:
            pandas.DataFrame: The filtered DataFrame.
        """
        # Convert start_time and end_time to pandas.Timestamp with timezone
        start_time = pd.Timestamp(start_time).tz_convert(timezone)
        end_time = pd.Timestamp(end_time).tz_convert(timezone)
        
        # Convert DataFrame's '@timestamp' column to the specified timezone
        # df['@timestamp'] = df['@timestamp'].dt.tz_convert(timezone)
        
        # Filter rows based on the '@timestamp' column
        filtered_df = df[(df['@timestamp'] >= start_time) & (df['@timestamp'] <= end_time)]
        
        return filtered_df

    def filter_new_srcip(self, df1, df2):
        unique_srcip_df1 = df1['srcip'].unique()
        unique_srcip_df2 = df2['srcip'].unique()

        print("Unique IPs in df1:", len(unique_srcip_df1))
        print("Unique IPs in df2:", len(unique_srcip_df2))
        
        df_filtered = df2[~df2['srcip'].isin(unique_srcip_df1)]


        return df_filtered


    def summarize_ip_sessions(self, df):
        df_copy = df.copy()
        df_copy = df_copy.groupby(['srccountry', 'srcip']).size().reset_index(name='sessions')
        df_copy.sort_values(by='sessions', ascending=False, inplace=True)
        return df_copy

    def summarize_by_srccountry(self, df):
        df_copy = df.copy()
        summary_df = df_copy.groupby('srccountry').agg(
            num_sessions=('srcip', 'size'),
            unique_ips=('srcip', 'nunique'),
            average_sessions_per_ip=('srcip', lambda x: x.size / x.nunique())
        ).reset_index()
        
        summary_df.sort_values(by='num_sessions', ascending=False, inplace=True)

        return summary_df
    
    def summarize_by_srccountry_and_subnet(self, df):
        df_copy = df.copy()
        df_copy["subnet"] = df_copy["srcip"].map(lambda x: '.'.join(str(x).split('.')[:3]) + '.0/24')
        summary_df = df_copy.groupby(['srccountry', 'subnet']).agg(
            num_sessions=('srcip', 'size'),
            unique_ips=('srcip', 'nunique'),
            average_sessions_per_ip=('srcip', lambda x: x.size / x.nunique())
        ).reset_index()
        
        summary_df.sort_values(by='num_sessions', ascending=False, inplace=True)

        return summary_df
    
    def write_excel(self, path: pathlib.Path, df_map: dict[str, pd.DataFrame]):
        with pd.ExcelWriter(path=path, engine='openpyxl') as writer:
            for sheet_name, df in df_map.items():
                df.to_excel(excel_writer=writer, sheet_name=sheet_name, index=False)



