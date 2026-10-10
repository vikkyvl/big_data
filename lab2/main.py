from pyspark import SparkConf, SparkContext
from page_rank import PageRank
from map_reduce_page_rank import MapReducePageRank
from config import APP_NAME, SPARK_MASTER, INPUT_FILE, STANDARD_PAGE_RANK_NAME, MAP_REDUCE_PAGE_RANK_NAME, RUN_STANDARD, RUN_MAP_REDUCE, DEBUG, EXPERIMENT_TYPE, ExperimentType
from experiments.common import get_input_path, get_dataset_info, run_algorithm, compare_results
from experiments.datasets import run_datasets_experiment
from experiments.damping import run_damping_experiment
from experiments.epsilon import run_epsilon_experiment


def run_single(spark_context):
    input_path = get_input_path(INPUT_FILE)
    pages, links = get_dataset_info(spark_context, input_path)

    print("\n========== DATASET INFORMATION ==========")
    print(f"File: {INPUT_FILE}")
    print(f"Pages: {pages}")
    print(f"Links: {links}")

    standard_result = None
    map_reduce_result = None

    if RUN_STANDARD:
        standard_result = run_algorithm(STANDARD_PAGE_RANK_NAME, PageRank(debug=DEBUG), spark_context, input_path)

    if RUN_MAP_REDUCE:
        map_reduce_result = run_algorithm(MAP_REDUCE_PAGE_RANK_NAME, MapReducePageRank(debug=DEBUG), spark_context, input_path)

    if standard_result is not None and map_reduce_result is not None:
        compare_results(standard_result, map_reduce_result)


def main():
    conf = SparkConf().setAppName(APP_NAME).setMaster(SPARK_MASTER)
    spark_context = SparkContext(conf=conf)
    spark_context.setLogLevel("ERROR")

    try:
        if EXPERIMENT_TYPE == ExperimentType.NONE:
            run_single(spark_context)
        elif EXPERIMENT_TYPE == ExperimentType.DATASETS:
            run_datasets_experiment(spark_context)
        elif EXPERIMENT_TYPE == ExperimentType.DAMPING:
            run_damping_experiment(spark_context)
        elif EXPERIMENT_TYPE == ExperimentType.EPSILON:
            run_epsilon_experiment(spark_context)
        else:
            raise ValueError(f"Unknown experiment type: {EXPERIMENT_TYPE}")
    finally:
        spark_context.stop()


if __name__ == "__main__":
    main()