from config import EXPERIMENT_DATASET, EPSILON_VALUES
from experiments.common import get_input_path, run_both_algorithms, print_experiment_settings, print_top_pages, calculate_rank_difference


def print_epsilon_experiment_results(results, reference_epsilon):
    print("\n========== EPSILON EXPERIMENT RESULTS ==========")
    print(f"Reference epsilon: {reference_epsilon:.0e}")
    print(f"{'Epsilon':<12} {'Algorithm':<12} {'Iterations':>12} {'Delta':>14} {'Max diff':>14} {'Ref diff':>14} {'Time (s)':>12}")
    print("-" * 100)

    for epsilon, standard, map_reduce, max_difference, standard_ref_diff, map_reduce_ref_diff in results:
        print(f"{epsilon:<12.0e} {'Standard':<12} {standard[1]:>12} {standard[2]:>14.8f} {max_difference:>14.10f} {standard_ref_diff:>14.10f} {standard[3]:>12.2f}")
        print(f"{'':<12} {'MapReduce':<12} {map_reduce[1]:>12} {map_reduce[2]:>14.8f} {max_difference:>14.10f} {map_reduce_ref_diff:>14.10f} {map_reduce[3]:>12.2f}")
        print("-" * 100)


def run_epsilon_experiment(spark_context):
    results = []
    input_path = get_input_path(EXPERIMENT_DATASET)

    print_experiment_settings(EXPERIMENT_DATASET)

    for epsilon in EPSILON_VALUES:
        print(f"\nRunning experiment: epsilon = {epsilon:.0e}")

        standard, map_reduce, max_difference = run_both_algorithms(spark_context, input_path, epsilon=epsilon)

        print_top_pages(standard, "epsilon", f"{epsilon:.0e}")
        results.append((epsilon, standard, map_reduce, max_difference))

    reference_epsilon = min(EPSILON_VALUES)
    reference_result = next(standard for epsilon, standard, map_reduce, max_difference in results if epsilon == reference_epsilon)

    comparison_results = []

    for epsilon, standard, map_reduce, max_difference in results:
        standard_ref_diff = calculate_rank_difference(standard[0], reference_result[0])
        map_reduce_ref_diff = calculate_rank_difference(map_reduce[0], reference_result[0])
        comparison_results.append((epsilon, standard, map_reduce, max_difference, standard_ref_diff, map_reduce_ref_diff))

    print_epsilon_experiment_results(comparison_results, reference_epsilon)