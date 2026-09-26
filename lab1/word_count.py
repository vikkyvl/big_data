import re
from utils import WORD_PATTERN
from pyspark import SparkConf, SparkContext
from config import SPARK_MASTER, WORD_COUNT_APP_NAME


class WordCount:
    def __init__(self):
        conf = SparkConf().setAppName(WORD_COUNT_APP_NAME).setMaster(SPARK_MASTER)
        self.sc = SparkContext.getOrCreate(conf)

    def read_input(self, path):
        return self.sc.textFile(path)

    @staticmethod
    def normalize_word(word):
        return word.lower()

    @classmethod
    def extract_words(cls, line):
        return re.findall(WORD_PATTERN, cls.normalize_word(line))

    @staticmethod
    def create_pair(word):
        return word, 1

    @classmethod
    def split_words(cls, lines):
        return lines.flatMap(cls.extract_words)

    @classmethod
    def map_words(cls, words):
        return words.map(cls.create_pair)

    @staticmethod
    def reduce_words(words):
        return words.reduceByKey(lambda a, b: a + b)

    def run(self, path):
        data = self.read_input(path)
        data = self.split_words(data)
        data = self.map_words(data)
        data = self.reduce_words(data)

        return data