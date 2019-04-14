# Overview
log_analyzer.py - is an utility script that lurks in the LOG_DIR for a nginx log file, calculates most time consuming http requests and build up a report in the REPORT_DIR folder. Runs only in Python 3.x.

Arguments:
- --config <cfgfile> - optional path to configuration file that overwrites a default configuration

How to run:
```
> cat <<EOT >> my.conf
[DEFAULT]
LOG_DIR=./mylog
REPORT_SIZE=10
EOT

> python3 log_analyzer.py --config my.conf
```
