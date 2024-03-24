import time
import logging


class MyProfiler:
    def __init__(self):
        self.dict = {}
        self.time_metrics = {}
        self.counters = {}
        self.heartbeat_enable = False

    def add_counters(self, *counter_names):
        for name in counter_names:
            self.counters[name] = 0

    def add_time_metrics(self, *metric_names):
        for name in metric_names:
            self.time_metrics[name] = {"start_time": 0, "end_time": 0, "kind": "fixed"}

    def add_time_metric(self, metric_name, kind="fixed"):
        self.time_metrics[metric_name] = {"start_time": 0, "end_time": 0, "kind": kind}
        return self.time_metrics[metric_name]

    def start_time_metric(self, metric_name):
        metric = self.time_metrics.get(metric_name)
        if metric is None:
            metric = self.add_time_metric(metric_name, "temporal")
        metric["start_time"] = time.monotonic()

    def stop_time_metric(self, metric_name):
        end_time = time.monotonic()
        metric = self.time_metrics[metric_name]
        metric["end_time"] = end_time

    def report_rate(self, time_metric_name, total_processed, unit):
        metric = self.time_metrics[time_metric_name]
        time_delta = metric["end_time"] - metric["start_time"]
        if time_delta > 0:
            rate = total_processed / time_delta
            logging.info(
                f"{time_metric_name}: processed {total_processed} {unit} in {time_delta:.4f} seconds ({rate:.2f} {unit}/s)."
            )
        else:
            rate = total_processed
            logging.info(
                f"{time_metric_name}: processed {total_processed} {unit} in less than {time_delta:.4f} seconds."
            )

    def delete_temporal_metrics(self):
        keys = [
            key
            for key, value in self.time_metrics.items()
            if value.get("kind") == "temporal"
        ]
        for kname in keys:
            del self.time_metrics[kname]

    def get_counter_value(self, counter_name):
        return self.counters[counter_name]

    def reset_counters(self, *keys):
        for key in keys:
            self.counters[key] = 0

    def counter_increment(self, name):
        self.counters[name] += 1
        if self.heartbeat_enable and name == self.heartbeat_counter:
            self.eval_heartbeat(self.counters[name])

    def set_heartbeat_signal(
        self,
        heartbeat_counter,
        heartbeat_time_metric,
        heartbeat_threshold,
        heartbeat_total,
    ):
        self.heartbeat_threshold = heartbeat_threshold
        self.heartbeat_counter = heartbeat_counter
        self.heartbeat_total = heartbeat_total
        self.heartbeat_enable = heartbeat_total > heartbeat_threshold
        self.heartbeat_time_metric = heartbeat_time_metric

    def eval_heartbeat(self, current_count):
        modulo = current_count % self.heartbeat_threshold
        if modulo == 0:
            heartbeat_elapsed_time = (
                time.monotonic()
                - self.time_metrics[self.heartbeat_time_metric]["start_time"]
            )
            heartbeat_rate = self.heartbeat_threshold / heartbeat_elapsed_time
            logging.info(
                f"Current {current_count}/{self.heartbeat_total}. Last {self.heartbeat_threshold} processed in {heartbeat_elapsed_time:.2f} seconds ({heartbeat_rate:.2f}/s). "
            )

        elif (
            self.heartbeat_total > self.heartbeat_threshold
            and current_count == self.heartbeat_total
        ):
            heartbeat_elapsed_time = (
                time.monotonic()
                - self.time_metrics[self.heartbeat_time_metric]["start_time"]
            )
            heartbeat_rate = modulo / heartbeat_elapsed_time
            logging.info(
                f"Current {current_count}/{self.heartbeat_total}. Last {modulo} processed in {heartbeat_elapsed_time:.2f} seconds ({heartbeat_rate:.2f}/s). "
            )

    def report_counters(self):
        for cname in self.counters:
            logging.info(f"Total {cname}: {self.counters[cname]}.")
