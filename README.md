# Firefox Usage Report

This repository contains code and analysis supporting the Firefox Usage Report, also known as the Firefox Public Data Project. It is modelled heavily after the [Firefox Hardware Report](https://hardware.metrics.mozilla.com/). The result will be a public webpage that tracks various metrics over time, however this repo is focused on the data extraction code for now.


# Job Schedule

We intend to run this job once a week, accounting for usage in the previous 7 days with the exception of MAU and YAU, which count users over a 28 and 365 day period, respectively.


# Developing

## Run the Job

To initiate a test run of this job, you can clone this repo onto an ATMO cluster. First run

	$ pip install py4j --upgrade

from your cluster console to get the latest version of `py4j`.


Next, clone the repo, and from the repo's top-level directory, run:
	
	$ python usage_report/usage_report.py --date [some date, i.e. 20180201] --no-output
	
which will aggregate usage statics from the last 7 days by default. It is recommended when testing to specifiy the `--lag-days` flag to `1` for quicker iterations, i.e

	$ python usage_report/usage_report.py --date 20180201 --lag-days 1 --no-output
	
*Note: there is currently no output to S3, so testing like this is not a problem. However when testing runs in this way, always make sure to include the flag* `--no-output`
	
## Testing

Each metric has it's own set of unit tests. Code to extract a particular metric are found in `.py` files in `usage_report/utils/`, which are integrated in `usage_report/usage_report.py`.

To run these tests, first ensure to install `tox` and `snappy`:

	$ pip install tox
	$ brew install snappy
	

Once installed, you can simply run 

	$ tox
	
from the repo's top-level directory. This command invokes `pytest` and also runs `flake8` tests on the codebase, which is a style linter. **Be sure that all tests pass for `pytest` *and* `flake8` before pushing any updated code.**

