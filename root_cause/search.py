import copy
import pandas as pd
from preparation import DatasetPreparation
import re
from settings import SearchStrategy
from settings import SearchSettings


class RootCauseEntry:
    def __init__(self, line_id: int, message: pd.Series, strategies: list[SearchStrategy]):
        self.line_id = line_id
        self.message = message
        self.strategies = strategies


class RootCauseSearch:
    def __init__(self, settings: SearchSettings):
        self.settings = settings
        self.log_messages = None
        self.root_cause = []
        self.reset_data(False)

    def reset_data(self, is_reload: bool):
        # Reload dataset after each search. This is necessary because it is changed by the search. However, the steps
        # for preparing the dataset are only executed during the first loading in the constructor of this class. After
        # that they are skipped to increase the speed.
        if self.log_messages is None:
            self.log_messages = DatasetPreparation(self.settings).get(is_reload)

            # Remove results of last search.
            self.root_cause = []

    def search(self, error_line_id: int):
        self.reset_data(True)

        error = self.log_messages.get_by_id(error_line_id)
        for strategy in self.settings.strategies:
            self.search_strategy(error_line_id, error, strategy)
        self.add_to_root_cause(error_line_id, 0)

        self.root_cause = sorted(self.root_cause, key=lambda entry: entry.line_id)
        self.settings.output.print_root_cause(error_line_id, self.root_cause)

        # Remove the loaded dataset in preparation for the next search.
        self.log_messages = None

        return self.root_cause

    def search_strategy(self, error_line_id: int, error: pd.Series, strategy: SearchStrategy):
        self.settings.output.print_headline(
            f'Trying search strategy "{strategy.intersection_occurrences_col}|{strategy.intersection_col}|{strategy.hidden_occurrences_col}|{strategy.uniqueness_col}|{strategy.max_noise}"'
        )

        # Look for occurrences of the same error for creating the intersection.
        intersection_occurrences = self.log_messages.get_by_value(
            strategy.intersection_occurrences_col, error[strategy.intersection_occurrences_col]
        )
        self.settings.output.print_status(
            f'{len(intersection_occurrences)} error occurrences found. They are used to create a intersection of all time windows before the error'
        )
        if len(intersection_occurrences) < 2:
            return

        # Create intersection of time windows before the occurrences of the same error.
        intersection = self.log_messages.time_windows_intersection(
            strategy.intersection_col, intersection_occurrences['timestamp'], strategy.window_seconds
        )
        self.settings.output.print_status(f'{len(intersection)} values in intersection of time windows found')
        if len(intersection) < 2:
            return

        # Extract time window before error.
        error_window = self.log_messages.time_window(error['timestamp'], strategy.window_seconds)

        # Find error occurrences for time windows, that are skipped in uniqueness check of root cause candidates
        hidden_occurrences = self.log_messages.get_by_value(
            strategy.hidden_occurrences_col, error[strategy.hidden_occurrences_col]
        )
        self.settings.output.print_status(
            f'{len(hidden_occurrences)} error occurrences found. They are used to mark the time windows that are skipped in the uniqueness check for root cause candidates'
        )
        if len(hidden_occurrences) < 2:
            return

        # Count values outside time windows.
        outside_windows_count = self.log_messages.count_outside_time_windows(
            strategy.uniqueness_col, hidden_occurrences['timestamp'], strategy.window_seconds
        )

        #
        added_count = 0
        for intersection_value in intersection:
            candidates = error_window[(error_window[strategy.intersection_col] == intersection_value)]
            candidates = candidates[strategy.uniqueness_col].drop_duplicates()

            for line_id, candidate in candidates.items():
                if type(candidate) != type(outside_windows_count.index[0]):
                    raise TypeError('Uniqueness column has different data type.')
                if line_id == error_line_id:
                    continue

                found_with_noise = outside_windows_count.get(candidate, 0)
                if found_with_noise <= strategy.max_noise:
                    if self.add_to_root_cause(line_id, found_with_noise, strategy):
                        added_count += 1

        self.settings.output.print_completion(f'{added_count} lines added to root cause')

    def add_to_root_cause(self, line_id: int, found_with_noise: int, strategy: SearchStrategy = None) -> bool:
        log_message = self.log_messages.get_by_id(line_id)

        for fil in self.settings.service_filter:
            if re.search(fil, str(log_message['service'])) is not None:
                return False

        for fil in self.settings.content_filter:
            if re.search(fil, str(log_message['content'])) is not None:
                return False

        if self.settings.duplicate_filter_col is not None:
            existing_column_values = [entry.message[self.settings.duplicate_filter_col] for entry in self.root_cause]
            if log_message[self.settings.duplicate_filter_col] in existing_column_values:
                return False

        if strategy is not None:
            strategy = copy.deepcopy(strategy)
            strategy.found_with_noise = found_with_noise

        for entry in self.root_cause:
            if entry.line_id == line_id and strategy is not None:
                entry.strategies.append(strategy)
                return True

        strategies = []
        if strategy is not None:
            strategies = [strategy]

        self.root_cause.append(RootCauseEntry(line_id, log_message, strategies))
        return True
