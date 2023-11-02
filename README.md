# Root Cause Identification in Log Data

This repository contains a Python codebase for an experimental data-mining algorithm designed to identify root causes in
log data. Its aim is to evaluate the algorithm's effectiveness in a practical setting.

## How to Run the Project
To get the project up and running on your machine, follow these steps:

1. Clone the repository to your local machine.
2. Ensure that Docker is installed and running on your system.
3. Navigate to the root directory of the project via the command line.
4. Execute the command ./notebook-container up. This will build and run a Docker container specifically for this project.
5. Once the container is running, access the Jupyter Notebook environment by visiting http://localhost:8888 on your web browser.
6. In the /notebooks directory, you will find an example Jupyter Notebook that demonstrates the use of the code with an Android log dataset.

## Quick Start Guide

```python
from search import RootCauseSearch
from settings import SearchStrategy, SearchSettings
from output import DisplayNotebookOutput

# Refer to the Jupyter Notebook for all optional parameters.
search_settings = SearchSettings(
    dataset_name='my_log_data',
    storage_dir='../storage',
    source_csv_file='../storage/my_log_data.source.csv',  # Path to the CSV file containing the log data
    drain_config_file='../drain3.ini',
    output=DisplayNotebookOutput(), # Replace with "DisplayNoOutput" for result objects only.
    strategies=[SearchStrategy()]
)
root_cause = RootCauseSearch(search_settings)

# Analyze the log data to find root causes for an error message at line 542657.
root_cause = root_cause.search(542657)
```

Please ensure that the CSV file passed to the algorithm contains the required columns:
`line_id`, `timestamp`, `service` and `content`.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
