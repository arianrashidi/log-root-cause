from abc import ABC, abstractmethod


class DisplayOutput(ABC):
    @abstractmethod
    def progress_bars(self) -> bool:
        pass

    @abstractmethod
    def print_headline(self, text: str):
        pass

    @abstractmethod
    def print_next(self, text: str):
        pass

    @abstractmethod
    def print_status(self, text: str):
        pass

    @abstractmethod
    def print_completion(self, text: str):
        pass

    @abstractmethod
    def print_root_cause(self, error_line_id: int, root_cause: list):
        pass

    @abstractmethod
    def print_root_cause_entry(self, error_line_id: int, entry):
        pass

    @abstractmethod
    def colored_string(self, text: str, code: int):
        pass

    @abstractmethod
    def colored_bold_string(self, text: str, code: int):
        pass

    @staticmethod
    def right_trim(text: str, remove: str) -> str:
        if text.endswith(remove):
            text = text[:-len(remove)]
        return text


class DisplayNotebookOutput(DisplayOutput):
    def progress_bars(self) -> bool:
        return True

    def print_headline(self, text: str):
        print(f'\x1b[30;1m{text}:\x1b[0m')

    def print_next(self, text: str):
        print(f'↻ {text} ...')

    def print_status(self, text: str):
        print(f'ℹ {text}.')

    def print_completion(self, text: str):
        print(f'✓ {text}.')

    def print_root_cause(self, error_line_id: int, root_cause: list):
        if len(root_cause) > 1:
            self.print_headline('\nResults')
        else:
            self.print_completion('No root cause found')
            return

        for entry in root_cause:
            self.print_root_cause_entry(error_line_id, entry)

    def print_root_cause_entry(self, error_line_id: int, entry):
        message = entry.message
        color = 31 if entry.line_id == error_line_id else 32

        message_output = ''
        entry_info = {
            'Line': str(entry.line_id),
            'Timestamp': str(message['timestamp']),
            'Service': message['service'],
            'Template': message['template'],
            'Content': self.right_trim(message['content'], ':')
        }
        for info_key, info_value in entry_info.items():
            message_output += '\n' + self.colored_bold_string(info_key + ':', color) + ' ' + self.colored_string(
                info_value,
                color
            )

        if len(entry.strategies) > 0:
            message_output += '\n' + self.colored_bold_string('Found with strategies:', color)
        for strategy in entry.strategies:
            message_output += '\n' + self.colored_string(
                f'- {strategy.intersection_occurrences_col}|{strategy.intersection_col}|{strategy.hidden_occurrences_col}|{strategy.uniqueness_col}|{strategy.max_noise}|{strategy.found_with_noise}',
                color
            )

        print(message_output)

    def colored_string(self, text: str, code: int):
        return f'\x1b[{code}m{text}\x1b[0m'

    def colored_bold_string(self, text: str, code: int):
        return f'\x1b[{code};1m{text}\x1b[0m'


class DisplayNoOutput(DisplayOutput):
    def progress_bars(self) -> bool:
        return False

    def print_headline(self, text: str):
        pass

    def print_next(self, text: str):
        pass

    def print_status(self, text: str):
        pass

    def print_completion(self, text: str):
        pass

    def print_root_cause(self, error_line_id: int, root_cause: list):
        pass

    def print_root_cause_entry(self, error_line_id: int, entry):
        pass

    def colored_string(self, text: str, code: int):
        pass

    def colored_bold_string(self, text: str, code: int):
        pass
