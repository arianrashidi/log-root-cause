from collections.abc import Callable
from datetime import datetime
import os
from pandarallel import pandarallel
import pandas as pd
from tqdm.auto import tqdm


class LogMessages:
    def __init__(self, dataframe: pd.DataFrame):
        self.log_messages = dataframe

        self.log_messages = dataframe.convert_dtypes()  # Use efficient data types
        if 'timestamp' in self.log_messages.columns:
            self.log_messages['timestamp'] = pd.to_datetime(self.log_messages['timestamp'])

        self.required_columns = ['timestamp', 'content', 'service', 'template', 'service_template_id']
        self.pandarallel_initialized = False
        self.tqdm_initialized = False

    def normalize_column_names(self):
        self.log_messages.columns = self.log_messages.columns.str.strip()
        self.log_messages.columns = self.log_messages.columns.str.lower()
        self.log_messages.columns = self.log_messages.columns.str.replace('-', '_', regex=True)
        self.log_messages.columns = self.log_messages.columns.str.replace(' ', '_', regex=True)

    def get_by_id(self, line_id: int) -> pd.Series:
        return self.log_messages.loc[line_id]

    def get_by_value(self, column: str, value) -> pd.DataFrame:
        return self.log_messages.loc[self.log_messages[column] == value]

    def to_csv(self, csv_file: str):
        self.log_messages.to_csv(csv_file, index_label='line_id')

    def create_empty_column(self, column: str, default_value):
        self.log_messages[column] = default_value

    def apply_on_column(self, column: str, func: Callable, show_progress: bool):
        if show_progress:
            self.init_tqdm()
            self.log_messages[column].progress_apply(func)
        else:
            self.log_messages[column].apply(func)

    def init_tqdm(self):
        if not self.tqdm_initialized:
            tqdm.pandas()  # Shows progress bar
            self.tqdm_initialized = True

    def parallel_apply_on_rows(self, func: Callable, parallel_processing: bool, show_progress: bool, *args, **kwargs):
        chunk_size = 2_000_000
        messsages_count = len(self.log_messages)
        if chunk_size > messsages_count:
            chunk_size = messsages_count

        if parallel_processing:
            self.init_pandarallel(show_progress)
            for i in range(0, messsages_count, chunk_size):
                self.log_messages.iloc[i:i + chunk_size] = self.log_messages.iloc[i:i + chunk_size].parallel_apply(
                    func,
                    args=args,
                    **kwargs,
                    axis=1
                )
        else:
            if show_progress:
                self.init_tqdm()
            for i in range(0, messsages_count, chunk_size):
                self.log_messages.iloc[i:i + chunk_size] = self.log_messages.iloc[i:i + chunk_size].progress_apply(
                    func,
                    args=args,
                    **kwargs,
                    axis=1
                )

    def init_pandarallel(self, show_progress: bool):
        if not self.pandarallel_initialized:
            os.environ['JOBLIB_TEMP_FOLDER'] = '/tmp'
            pandarallel.initialize(
                nb_workers=int(os.cpu_count()) - 1,
                progress_bar=show_progress,
                use_memory_fs=False,
                verbose=1
            )
            self.pandarallel_initialized = True

    def ensure_required_columns_exist(self, template: bool):
        required = self.required_columns.copy()
        if not template:
            required.remove('template')
            required.remove('service_template_id')

        for column in required:
            if not column in self.log_messages.columns:
                raise ValueError(f'{column} column not found.')

    def remove_unnecessary_columns(self):
        self.log_messages = self.log_messages.filter(items=self.required_columns)

    def combine_daytime_to_timestamps(self):
        columns = self.log_messages.columns
        if 'day' in columns and 'time' in columns and not 'timestamp' in columns:
            self.log_messages['timestamp'] = self.log_messages['day'] + ' ' + self.log_messages['time']

    def validate_timestamp_format(self):
        datetime.strptime(str(self.log_messages.iloc[0]['timestamp']), '%Y-%m-%d %H:%M:%S.%f')

    def validate_timestamp_order(self):
        if self.log_messages.iloc[0]['timestamp'] > self.log_messages.iloc[-1]['timestamp']:
            raise ValueError('Timestamps are not in descending order.')

    def add_service_template_ids(self):
        self.log_messages['service_template_id'] = self.log_messages.groupby(
            self.log_messages[['service', 'template']].apply(frozenset, axis=1)
        ).ngroup() + 1

    def count_outside_time_windows(self, column: str, end_times: pd.Series, seconds: int) -> pd.Series:
        end_times = end_times.to_list()
        end_times.append(self.log_messages['timestamp'].max())  # Add last timestamp of data to end_times just in case.
        log_messages = self.log_messages

        for end_time in end_times:
            start_time = end_time - pd.Timedelta(seconds=seconds)
            log_messages = log_messages[
                (log_messages['timestamp'] < start_time) | (log_messages['timestamp'] > end_time)
                ]

        return log_messages[column].value_counts()

    def time_windows_intersection(self, column: str, end_times: pd.Series, seconds: int) -> list:
        windows = self.time_windows(end_times, seconds)

        if len(windows) == 0:
            return []
        if len(windows) == 1:
            return windows[0][column].to_list()

        on = [column]
        intersection = windows[0][on]
        for dataframe in windows[1:]:
            intersection = intersection.merge(dataframe[on], on=on, how='inner').drop_duplicates()

        return intersection[column].to_list()

    def time_windows(self, end_times: pd.Series, seconds: int) -> list[pd.DataFrame]:
        windows = []
        for end_time in end_times:
            windows.append(self.time_window(end_time, seconds))

        return windows

    def time_window(self, end_time: pd.Timestamp, seconds: int) -> pd.DataFrame:
        start_time = end_time - pd.Timedelta(seconds=seconds)

        return self.log_messages[
            (self.log_messages['timestamp'] >= start_time) & (self.log_messages['timestamp'] <= end_time)
            ]
