from messages import LogMessages
import os
import pandas as pd
from parser import TemplateParser
from settings import SearchSettings


class DatasetPreparation:
    def __init__(self, settings: SearchSettings):
        self.settings = settings

    def get(self, is_reload: bool) -> LogMessages:
        log_messages = self.read_csv(is_reload)

        if is_reload:
            action = 'Dataset loaded'
        else:
            log_messages = self.prepare_for_template_clustering(log_messages)
            self.create_template_clusters(log_messages)
            log_messages = self.assign_templates(log_messages)
            self.delete_pre_clustering_data()
            action = 'Dataset loaded and prepared'

        self.settings.output.print_completion(action)
        return log_messages

    def read_csv(self, is_reload: bool) -> LogMessages:
        if not self.settings.pre_clustering_csv_file_exists() and not self.settings.post_clustering_csv_file_exists():
            csv_file = self.settings.source_csv_file
        elif not self.settings.post_clustering_csv_file_exists():
            csv_file = self.settings.pre_clustering_csv_file
        else:
            csv_file = self.settings.post_clustering_csv_file

        if is_reload:
            action = f'Reloading dataset from CSV file'
        else:
            action = f'Loading dataset from CSV file and preparing it'
        self.settings.output.print_headline(action)

        log_messages = LogMessages(pd.read_csv(csv_file, index_col='line_id'))

        if self.settings.post_clustering_csv_file_exists():
            log_messages.ensure_required_columns_exist(True)
        elif self.settings.pre_clustering_csv_file_exists():
            log_messages.ensure_required_columns_exist(False)

        return log_messages

    def prepare_for_template_clustering(self, log_messages: LogMessages) -> LogMessages:
        self.settings.output.print_next('Preparing dataset for template clustering')

        if not self.settings.pre_clustering_csv_file_exists() and not self.settings.post_clustering_csv_file_exists():
            log_messages.normalize_column_names()
            log_messages.combine_daytime_to_timestamps()
            log_messages.ensure_required_columns_exist(False)
            log_messages.remove_unnecessary_columns()
            log_messages.validate_timestamp_format()
            log_messages.validate_timestamp_order()
            log_messages.to_csv(self.settings.pre_clustering_csv_file)

        return log_messages

    def create_template_clusters(self, log_messages: LogMessages):
        self.settings.output.print_next('Creating template clusters')

        if not self.settings.drain_state_file_file_exists() and not self.settings.post_clustering_csv_file_exists():
            if os.path.isfile(self.settings.temporary_drain_state_file):
                os.remove(self.settings.temporary_drain_state_file)

            parser = TemplateParser(self.settings.drain_config_file, self.settings.temporary_drain_state_file)
            log_messages.apply_on_column(
                'content',
                lambda content: parser.add_log_message(content),
                self.settings.output.progress_bars()
            )

            os.rename(self.settings.temporary_drain_state_file, self.settings.drain_state_file)

    def assign_templates(self, log_messages: LogMessages) -> LogMessages:
        self.settings.output.print_next('Assigning the templates to their log messages')

        if not self.settings.post_clustering_csv_file_exists():
            parser = TemplateParser(self.settings.drain_config_file, self.settings.drain_state_file)
            log_messages.create_empty_column('template', None)
            log_messages.parallel_apply_on_rows(
                self.assign_template,
                self.settings.parallel_processing,
                self.settings.output.progress_bars(),
                parser
            )
            log_messages.add_service_template_ids()
            log_messages.to_csv(self.settings.post_clustering_csv_file)

        return log_messages

    @staticmethod
    def assign_template(row, parser):
        row['template'] = parser.get_template(row['content'])
        return row

    def delete_pre_clustering_data(self):
        if os.path.isfile(self.settings.pre_clustering_csv_file):
            os.remove(self.settings.pre_clustering_csv_file)
