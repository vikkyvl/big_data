import os
from enum import Enum


class ExperimentType(Enum):
    NONE = "none"
    DATASETS = "datasets"
    DAMPING = "damping"
    EPSILON = "epsilon"


APP_NAME = "PageRank"
SPARK_MASTER = "local[*]"

DAMPING_FACTOR = 0.85
EPSILON = 0.000001
MAX_ITERATIONS = 100

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

INPUT_FILE = "mini.txt"

STANDARD_PAGE_RANK_NAME = "STANDARD PAGE RANK"
MAP_REDUCE_PAGE_RANK_NAME = "MAP REDUCE PAGE RANK"

RUN_STANDARD = True
RUN_MAP_REDUCE = True

DEBUG = False
DEBUG_SAMPLE_SIZE = 10

SHOW_TOP_RANKS = True
TOP_RANKS_COUNT = 10

EXPERIMENT_TYPE = ExperimentType.EPSILON

EXPERIMENT_DATASETS = [
    "mini.txt",
    "wiki-Vote.txt",
    "soc-Epinions1.txt",
    "web-NotreDame.txt",
    "web-Stanford.txt"
]

EXPERIMENT_DATASET = "wiki-Vote.txt"

DAMPING_VALUES = [0.50, 0.65, 0.75, 0.85, 0.95]
EPSILON_VALUES = [1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8]