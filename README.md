# TEST ETL for CEPS

# Installation

To install the project and it's dependencies. Clone the project and run in your terminal:

```shell
$ poetry install
```

OBS: I am considering that you already have `Poetry` and `Docker` with `compose` plugin.

# Run Scripts

This project use taskipy to run its scripts.

There is two main scripts, one to merge all CEP data to just one file and other to use the merged data and query a CEP API to get more info about the CEP and save the data.

To run the merge script you should run in your terminal:

```shell
$ task merge_ceps
```

To run the ETL in the merged data run in your terminal:

```shell
$ task run
```

OBS: The second task will make a database available before running the ETL
