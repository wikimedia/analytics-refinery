# Analytics Refinery Python Module

## Usage:

The ```refinery/python``` directory should be in your ```PYTHONPATH```
in order to propery import this module.

```
export PYTHONPATH=/path/to/analytics/refinery/python
````

This env variable should be set for shell accounts on nodes in
the Wikimedia Analytics Cluster automatically.

You should then be able to ```import refinery``` in your python code.

## Test:

Some unit tests are run by calling the executable. Here is an example:

```shell
cd analytics-refinery  # Your are located in the root of the repo.
conda env create -n analytics-refinery -f python/conda-environment.yml
conda activate analytics-refinery
export PYTHONPATH=python  # As suggested before.
python bin/refinery-drop-older-than  # or whichever script with unit tests you want to run
```

You could also test the bin in production with a dryrun by omitting the `execute` parameter. e.g.:

```shell
bin/refinery-drop-older-than \
  --verbose \
  --older-than='400' \
  --allowed-interval='9999' \
  --skip-trash \
  --base-path='/wmf/data/raw/webrequests_data_loss' \
  --path-format='((upload|text|test_text)(/(?P<year>[0-9]+)(/(?P<month>[0-9]+)(/(?P<day>[0-9]+)(/(?P<hour>[0-9]+)(/(WARNING|ERROR))?)?)?)?)?)?'
```
