from pyspark import SparkConf, SparkContext
from config import SPARK_MASTER, LOGS_ANALYSIS_APP_NAME


STATUS_SUCCESS = "success"
STATUS_ERROR = "error"
HTTP_ERROR_THRESHOLD = 400


class LogsAnalysis:
    def __init__(self):
        conf = SparkConf().setAppName(LOGS_ANALYSIS_APP_NAME).setMaster(SPARK_MASTER)
        self.sc = SparkContext.getOrCreate(conf)

    def read_input(self, path):
        return self.sc.textFile(path)

    @staticmethod
    def parse_log(line):
        parts = [part.strip() for part in line.split(",")]

        if len(parts) < 4:
            return None

        user_id = parts[1]
        status_code = int(parts[3])

        return user_id, status_code

    @staticmethod
    def map_status(record):
        user_id, status_code = record

        if status_code < HTTP_ERROR_THRESHOLD:
            counts = {STATUS_SUCCESS: 1, STATUS_ERROR: 0}
        else:
            counts = {STATUS_SUCCESS: 0, STATUS_ERROR: 1}

        return user_id, counts

    @classmethod
    def parse_logs(cls, lines):
        return lines.map(cls.parse_log).filter(lambda x: x is not None)

    @classmethod
    def map_logs(cls, logs):
        return logs.map(cls.map_status)

    @staticmethod
    def merge_counts(a, b):
        return {
            STATUS_SUCCESS: a[STATUS_SUCCESS] + b[STATUS_SUCCESS],
            STATUS_ERROR: a[STATUS_ERROR] + b[STATUS_ERROR],
        }

    @classmethod
    def reduce_users(cls, logs):
        return logs.reduceByKey(cls.merge_counts)

    def run(self, path):
        data = self.read_input(path)
        data = self.parse_logs(data)
        data = self.map_logs(data)
        data = self.reduce_users(data)

        return data