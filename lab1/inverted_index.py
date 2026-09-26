import os
import re
from pyspark import SparkConf, SparkContext
from config import SPARK_MASTER, INVERTED_INDEX_APP_NAME
from utils import WORD_PATTERN


class InvertedIndex:
    def __init__(self):
        conf = SparkConf().setAppName(INVERTED_INDEX_APP_NAME).setMaster(SPARK_MASTER)
        self.sc = SparkContext.getOrCreate(conf)

    def read_input(self, path):
        return self.sc.wholeTextFiles(path)

    @staticmethod
    def split_document(document):
        file_path, content = document
        filename = os.path.basename(file_path)

        return [(filename, line_number, line) for line_number, line in enumerate(content.splitlines(), start=1)]

    @classmethod
    def split_documents(cls, documents):
        return documents.flatMap(cls.split_document)

    @staticmethod
    def normalize_word(word):
        return word.lower()

    @classmethod
    def extract_words(cls, record):
        filename, line_number, line = record
        words = re.findall(WORD_PATTERN, cls.normalize_word(line))

        return [(word, filename, line_number) for word in words]

    @classmethod
    def split_words(cls, lines):
        return lines.flatMap(cls.extract_words)

    @staticmethod
    def create_pair(record):
        word, filename, line_number = record
        position = f"{filename}@{line_number}"

        return word, position

    @classmethod
    def map_words(cls, words):
        return words.map(cls.create_pair)

    @staticmethod
    def shuffle_words(words):
        return words.groupByKey()

    @staticmethod
    def collect_positions(positions):
        return sorted(set(positions))

    @classmethod
    def reduce_words(cls, words):
        return words.mapValues(cls.collect_positions)

    def run(self, path):
        data = self.read_input(path)
        data = self.split_documents(data)
        data = self.split_words(data)
        data = self.map_words(data)
        data = self.shuffle_words(data)
        data = self.reduce_words(data)

        return data