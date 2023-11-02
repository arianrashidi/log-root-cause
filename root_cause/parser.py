from drain3.file_persistence import FilePersistence as DrainFilePersistence
from drain3 import TemplateMiner as DrainTemplateMiner
from drain3.template_miner_config import TemplateMinerConfig as DrainTemplateMinerConfig


class TemplateParser:
    def __init__(self, drain_config_file: str, drain_state_file: str):
        config = DrainTemplateMinerConfig()
        config.load(drain_config_file)
        persistence = DrainFilePersistence(drain_state_file)
        self.miner = DrainTemplateMiner(persistence, config)

    def add_log_message(self, content: str):
        self.miner.add_log_message(content)

    def get_template(self, content: str) -> str:
        cluster = self.miner.match(content)
        if cluster is not None:
            return cluster.get_template()
        return ''

    def extract_template_parameters(self, content: str, template: str) -> list:
        return self.miner.get_parameter_list(content, template)
