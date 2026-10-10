import os
import time
from page_rank import PageRank
from map_reduce_page_rank import MapReducePageRank
from config import DATA_DIR, STANDARD_PAGE_RANK_NAME, MAP_REDUCE_PAGE_RANK_NAME, DAMPING_FACTOR, EPSILON, MAX_ITERATIONS, SPARK_MASTER, SHOW_TOP_RANKS, TOP_RANKS_COUNT, DEBUG


def get_input_path(dataset):
    return f"file://{os.path.join(DATA_DIR, dataset)}"


def get_dataset_info(spark_context, input_path):
    edges = PageRank.read_edges(spark_context, input_path)

    try:
        pages = PageRank.collect_nodes(edges).count()
        links = edges.count()
        return pages, links
    finally:
        edges.unpersist(blocking=True)


def run_algorithm(title, algorithm, spark_context, input_path, show_results=True):
    print(f"\n========== {title} ==========")

    start_time = time.perf_counter()
    ranks, iterations, delta = algorithm.run(spark_context, input_path)
    execution_time = time.perf_counter() - start_time

    if show_results:
        if SHOW_TOP_RANKS:
            print(f"\nTop {TOP_RANKS_COUNT} pages:")

            for node, rank in ranks[:TOP_RANKS_COUNT]:
                print(f"{node}: {rank:.6f}")

        print(f"Iterations: {iterations}")
        print(f"Delta: {delta:.8f}")
        print(f"Execution time: {execution_time:.2f} s")

    return ranks, iterations, delta, execution_time


def calculate_rank_difference(first_ranks, second_ranks):
    first = dict(first_ranks)
    second = dict(second_ranks)

    if first.keys() != second.keys():
        raise ValueError("Algorithms returned different sets of pages")

    return max((abs(first[node] - second[node]) for node in first), default=0.0)


def calculate_max_difference(standard_result, map_reduce_result):
    return calculate_rank_difference(standard_result[0], map_reduce_result[0])


def compare_results(standard_result, map_reduce_result):
    max_difference = calculate_max_difference(standard_result, map_reduce_result)

    print("\n========== COMPARISON ==========")
    print(f"Max rank difference: {max_difference:.10f}")
    print(f"Standard execution time: {standard_result[3]:.2f} s")
    print(f"MapReduce execution time: {map_reduce_result[3]:.2f} s")


def run_both_algorithms(spark_context, input_path, damping_factor=DAMPING_FACTOR, epsilon=EPSILON):
    standard = run_algorithm(STANDARD_PAGE_RANK_NAME, PageRank(damping_factor=damping_factor, epsilon=epsilon, debug=DEBUG), spark_context, input_path, False)
    map_reduce = run_algorithm(MAP_REDUCE_PAGE_RANK_NAME, MapReducePageRank(damping_factor=damping_factor, epsilon=epsilon, debug=DEBUG), spark_context, input_path, False)
    max_difference = calculate_max_difference(standard, map_reduce)

    return standard, map_reduce, max_difference


def print_experiment_settings(dataset=None):
    print("\n========== EXPERIMENT SETTINGS ==========")

    if dataset is not None:
        print(f"Dataset: {dataset}")

    print(f"Damping factor: {DAMPING_FACTOR}")
    print(f"Epsilon: {EPSILON}")
    print(f"Max iterations: {MAX_ITERATIONS}")
    print(f"Spark master: {SPARK_MASTER}")
    print("=========================================")


def print_top_pages(result, parameter_name, parameter_value):
    if not SHOW_TOP_RANKS:
        return

    print(f"\nTop 5 pages for {parameter_name} = {parameter_value}")

    for node, rank in result[0][:5]:
        print(f"{node}: {rank:.6f}")


def print_experiment_table(results, parameter_name):
    print(f"\n========== {parameter_name.upper()} EXPERIMENT RESULTS ==========")
    print(f"{parameter_name:<20} {'Algorithm':<12} {'Iterations':>12} {'Delta':>14} {'Max diff':>14} {'Time (s)':>12}")
    print("-" * 90)

    for parameter, standard, map_reduce, max_difference in results:
        print(f"{str(parameter):<20} {'Standard':<12} {standard[1]:>12} {standard[2]:>14.8f} {max_difference:>14.10f} {standard[3]:>12.2f}")
        print(f"{'':<20} {'MapReduce':<12} {map_reduce[1]:>12} {map_reduce[2]:>14.8f} {max_difference:>14.10f} {map_reduce[3]:>12.2f}")
        print("-" * 90)