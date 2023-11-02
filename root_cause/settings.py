import functools
import os
from output import DisplayOutput


class SearchStrategy:
    def __init__(
            self,
            intersection_occurrences_col: str = 'content',
            intersection_col: str= 'service_template_id',
            hidden_occurrences_col: str = 'service_template_id',
            uniqueness_col: str = 'content',
            max_noise: int = 1,
            window_seconds: int = 2
    ):
        allowed_columns = ['content', 'service_template_id']
        if intersection_occurrences_col not in allowed_columns:
            raise ValueError(f'Intersection ocurrences column must be one of {allowed_columns}.')
        if intersection_col not in allowed_columns:
            raise ValueError(f'Intersection column must be one of {allowed_columns}.')
        if hidden_occurrences_col not in allowed_columns:
            raise ValueError(f'Hidden ocurrences column must be one of {allowed_columns}.')
        if uniqueness_col not in allowed_columns:
            raise ValueError(f'Uniqueness column must be one of {allowed_columns}.')

        self.intersection_occurrences_col = intersection_occurrences_col
        self.intersection_col = intersection_col
        self.hidden_occurrences_col = hidden_occurrences_col
        self.uniqueness_col = uniqueness_col
        self.window_seconds = window_seconds
        self.max_noise = max_noise
        self.found_with_noise = None


class SearchSettings:
    def __init__(
            self,
            dataset_name: str,
            source_csv_file: str,
            storage_dir: str,
            drain_config_file: str,
            strategies: list[SearchStrategy],
            service_filter: list[str],
            content_filter: list[str],
            output: DisplayOutput,
            duplicate_filter_col: str = 'service_template_id',
            parallel_processing: bool = False,
    ):
        self.validated_settings = {
            'storage_dir': storage_dir,
            'source_csv_file': source_csv_file,
            'drain_config_file': drain_config_file
        }

        self.dataset_name = dataset_name
        self.strategies = strategies
        self.service_filter = service_filter
        self.content_filter = content_filter
        self.duplicate_filter_col = duplicate_filter_col
        self.parallel_processing = parallel_processing
        self.output = output

    @functools.cached_property
    def storage_dir(self) -> str:
        if not os.path.isdir(self.validated_settings['storage_dir']):
            raise ValueError('Storage directory does not exist.')
        return self.validated_settings['storage_dir']

    @functools.cached_property
    def source_csv_file(self) -> str:
        if not os.path.isfile(self.validated_settings['source_csv_file']):
            raise ValueError('Source file with the unprepared log lines does not exist.')
        return self.validated_settings['source_csv_file']

    @functools.cached_property
    def pre_clustering_csv_file(self) -> str:
        return self.storage_dir + f'/{self.dataset_name}.pre_clustering.csv'

    def pre_clustering_csv_file_exists(self) -> bool:
        return os.path.isfile(self.pre_clustering_csv_file)

    @functools.cached_property
    def post_clustering_csv_file(self) -> str:
        return self.storage_dir + f'/{self.dataset_name}.post_clustering.csv'

    def post_clustering_csv_file_exists(self) -> bool:
        return os.path.isfile(self.post_clustering_csv_file)

    @functools.cached_property
    def drain_config_file(self) -> str:
        if not os.path.isfile(self.validated_settings['drain_config_file']):
            raise ValueError('Drain config file does not exist.')
        return self.validated_settings['drain_config_file']

    @functools.cached_property
    def drain_state_file(self) -> str:
        return self.storage_dir + f'/{self.dataset_name}.drain.bin'

    def drain_state_file_file_exists(self) -> bool:
        return os.path.isfile(self.drain_state_file)

    @functools.cached_property
    def temporary_drain_state_file(self) -> str:
        return self.storage_dir + f'/{self.dataset_name}.drain.tmp.bin'

    def temporary_drain_state_file_exists(self) -> bool:
        return os.path.isfile(self.temporary_drain_state_file)
