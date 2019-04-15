#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import datetime
import json
import configparser
import argparse
import logging
import gzip
import re

# unit-tests:
#  - на процент ошибок
#  - на лог с определенными данными

logging.basicConfig(format="[%(asctime)s] %(levelname).1s %(msg)s", filename=None)
log = logging.getLogger(__name__)
log.setLevel(logging.WARNING)

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log"
}


class Statistics:
    def __init__(self):
        self._samples = []
        self._total = 0.0

    def add_sample(self, time: float):
        self._samples.append(time)

    def process(self):
        self._samples.sort()
        self._total: float = sum(self._samples)

    def empty(self):
        return len(self._samples) > 0

    def count(self):
        return len(self._samples)

    def maximum(self):
        return self._samples[-1]

    def average(self):
        return self._total / len(self._samples)

    def median(self):
        return self._samples[len(self._samples) // 2]

    def total(self):
        return self._total


class Reporter:
    def __init__(self, default_config, config_filepath):
        self._config = dict(default_config)
        self.read_config(config_filepath)

        self._urls_stat = {}
        self._total_count = 0
        self._total_request_time = 0.0

    def read_config(self, path):
        if not os.path.exists(path):
            raise Exception("Could find config file: " + path)

        cp = configparser.ConfigParser()
        cp.read(path)

        # read the config file and overwrite the default values
        for name, value in cp.items("DEFAULT"):
            name = name.upper()
            if name in self._config:
                cast_to = type(self._config[name])
                self._config[name] = cast_to(value)
            else:
                self._config[name] = value

    @staticmethod
    def find_latest_log():
        log_suffix = ".log-"
        latest_date = datetime.datetime.utcfromtimestamp(0)
        result = None

        for dirpath, dirnames, filenames in os.walk(config["LOG_DIR"]):
            for filename in filenames:
                # is a log file?
                # example: nginx-access-ui.log-20170630
                i1 = filename.find(log_suffix)
                if i1 == -1:
                    log.info("Skipped file: %s, not a log" % filename)
                    continue

                i1 += len(log_suffix)
                date_str = filename[i1:i1+8]
                try:
                    date = datetime.datetime.strptime(date_str, "%Y%m%d")
                except Exception:
                    log.error("Wrong or unexpected date format in the file name: %s" % filename)
                    continue

                if date > latest_date:
                    latest_date = date
                    result = filename

        return result, latest_date

    def process_log(self, log_path, data_handler):
        def parse_nginx_log_line(line_str):
            methods = ('"GET', '"POST', '"PUT', '"DELETE', '"HEAD', '"CONNECT', '"OPTIONS', '"TRACE')
            http_suffix = " HTTP"

            i1, method = -1, ""
            for method in methods:
                i1 = line_str.find(method, 30)
                if i1 != -1:
                    break
            if i1 == -1:  # no HTTP method found
                return None, 0.0

            i1 += len(method) + 1

            # extract the URL
            i2 = line_str.find("?", i1)
            i3 = line_str.find(http_suffix, i1)
            if i2 != -1:
                i4 = min(i2, i3)
            else:
                i4 = i3
            if i4 == -1:
                return None, 0.0
            url = line_str[i1:i4]

            # extract the request time
            i5 = line_str.rfind(" ")
            request_time = float(line_str[i5 + 1:])

            return url, request_time

        parse_errors_threshold = 0.4

        log_ext = os.path.splitext(log_path)[1]
        open_func = {".gz": gzip.open}.get(log_ext, open)

        with open_func(log_path, "r") as f:
            parse_errors = 0

            for line in f:
                if self._total_count > 10 and float(parse_errors) / self._total_count > parse_errors_threshold:
                    log.error("Too many parse errors, stopped parsing")
                    return False

                # Example: 1.99.174.176 3b81f63526fa8  - [29/Jun/2017:03:50:22 +0300] "GET /
                # api/1/photogenic_banners/list/?server_name=WIN7RB4 HTTP/1.1" 200 12 "-"
                # "Python-urllib/2.7" "-" "1498697422-32900793-4708-9752770" "-" 0.133
                try:
                    line_str = line.decode("utf8")
                    url, request_time_sec = parse_nginx_log_line(line_str)
                    if url is None:
                        log.info("Skipped nginx log entry. Couldn't find a HTTP method in: %s" % line_str)
                        parse_errors += 1
                        continue

                    success = data_handler({"url": url, "request_time": request_time_sec})
                    if not success:
                        parse_errors += 1

                except Exception as e:
                    log.error("Failed to parse log line '%s' in file %s. Exception: %s" % (line_str, log_path, str(e)))
                    parse_errors += 1

        log.info("Done parsing")
        for key, ut in self._urls_stat.items():
            ut.process()

        return True

    def build_report(self, report_path):
        # generate json data
        data = []
        for url, ut in self._urls_stat.items():
            data.append({"count": ut.count(),
                         "time_avg": "%.3f" % ut.average(),
                         "time_max": "%.3f" % ut.maximum(),
                         "time_sum": ut.total(),
                         "url": url,
                         "time_med": "%.3f" % ut.median(),
                         "time_perc": "%.2f" % ((ut.total() / self._total_request_time) * 100.0),
                         "count_perc": "%.2f" % ((ut.count() / self._total_count) * 100.0)})

        data.sort(key=lambda item: item["time_sum"], reverse=True)
        for item in data:
            item["time_sum"] = "%.3f" % item["time_sum"]

        data = data[:config["REPORT_SIZE"]]  # cut out excess items

        table_json_text = json.dumps(data)

        # result text
        with open("report.html") as rtf:
            report_text = rtf.read()
        report_text = report_text.replace("$table_json", table_json_text)

        os.makedirs(os.path.dirname(report_path), exist_ok=True) # create intermediate directories
        with open(report_path, "w") as rf:
            rf.write(report_text)

    def count_request_time(self, data):
        try:
            url = data["url"]
            request_time = data["request_time"]
        except KeyError:
            return False

        try:
            stat = self._urls_stat[url]
        except KeyError:
            stat = Statistics()
            self._urls_stat[url] = stat

        stat.add_sample(request_time)
        self._total_request_time += request_time
        self._total_count += 1

        return True

    def run(self):
        latest_log_filename, log_date = Reporter.find_latest_log()
        if not latest_log_filename:
            return

        report_path = os.path.join(config["REPORT_DIR"], "report-" + log_date.strftime("%Y.%m.%d") + ".html")
        if os.path.exists(report_path):
            return

        log_path = os.path.join(config["LOG_DIR"], latest_log_filename)

        result = self.process_log(log_path, self.count_request_time)
        if not result:
            return

        self.build_report(report_path)


def main():
    try:
        parser = argparse.ArgumentParser(description="Parse nginx log files and build a report")
        parser.add_argument('--config', default="dflt.conf")
        args = parser.parse_args()

        Reporter(config, args.config).run()

    except Exception:
        log.exception("Unhandled exception")


if __name__ == "__main__":
    main()
