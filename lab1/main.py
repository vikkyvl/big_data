import os

from word_count import WordCount
from inverted_index import InvertedIndex
from logs_analysis import LogsAnalysis
from storage import HDFSStorage
from config import AlgorithmType


ALGORITHM = AlgorithmType.LOGS_ANALYSIS


def get_local_path(filename):
    base_path = os.path.dirname(os.path.abspath(__file__))
    return f"file://{base_path}/data/{filename}"


def format_word_count(record):
    word, count = record

    return f"{word}: {count}"


def format_inverted_index(record):
    word, positions = record

    return f"{word}: {', '.join(positions)}"


def format_logs_analysis(record):
    user_id, statistics = record

    return f"{user_id}: success={statistics['success']}, error={statistics['error']}"


def main():
    storage = HDFSStorage()

    if ALGORITHM == AlgorithmType.WORD_COUNT:
        algorithm = WordCount()
        result = algorithm.run(f"{get_local_path('A.txt')},{get_local_path('B.txt')}")
        result = result.map(format_word_count)
        storage.save(result, "word_count_output")

    elif ALGORITHM == AlgorithmType.INVERTED_INDEX:
        algorithm = InvertedIndex()
        result = algorithm.run(f"{get_local_path('lucrece.txt')},{get_local_path('orlando.txt')}")
        result = result.map(format_inverted_index)
        storage.save(result, "inverted_index_output")

    elif ALGORITHM == AlgorithmType.LOGS_ANALYSIS:
        algorithm = LogsAnalysis()
        result = algorithm.run(get_local_path("log*.txt"))
        result = result.map(format_logs_analysis)
        storage.save(result, "logs_analysis_output")


if __name__ == "__main__":
    main()