from config import EXPERIMENT_DATASETS
from experiments.common import get_input_path, get_dataset_info, run_both_algorithms, print_experiment_settings


def run_datasets_experiment(spark_context):
    results = []

    print_experiment_settings()

    for dataset in EXPERIMENT_DATASETS:
        input_path = get_input_path(dataset)
        pages, links = get_dataset_info(spark_context, input_path)

        print(f"\nRunning experiment: {dataset}")
        print(f"Pages: {pages}")
        print(f"Links: {links}")

        standard, map_reduce, max_difference = run_both_algorithms(spark_context, input_path)
        results.append((dataset, pages, links, standard, map_reduce, max_difference))

    print("\n========== DATASETS EXPERIMENT RESULTS ==========")
    print(f"{'Dataset':<20} {'Pages':>10} {'Links':>12} {'Algorithm':<12} {'Iterations':>10} {'Delta':>14} {'Max diff':>14} {'Time (s)':>12}")
    print("-" * 120)

    for dataset, pages, links, standard, map_reduce, max_difference in results:
        print(f"{dataset:<20} {pages:>10} {links:>12} {'Standard':<12} {standard[1]:>10} {standard[2]:>14.8f} {max_difference:>14.10f} {standard[3]:>12.2f}")
        print(f"{'':<20} {'':>10} {'':>12} {'MapReduce':<12} {map_reduce[1]:>10} {map_reduce[2]:>14.8f} {max_difference:>14.10f} {map_reduce[3]:>12.2f}")
        print("-" * 120)