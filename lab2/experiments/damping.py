from config import EXPERIMENT_DATASET, DAMPING_VALUES
from experiments.common import get_input_path, run_both_algorithms, print_experiment_settings, print_top_pages, print_experiment_table


def run_damping_experiment(spark_context):
    results = []
    input_path = get_input_path(EXPERIMENT_DATASET)

    print_experiment_settings(EXPERIMENT_DATASET)

    for damping_factor in DAMPING_VALUES:
        print(f"\nRunning experiment: d = {damping_factor}")

        standard, map_reduce, max_difference = run_both_algorithms(spark_context, input_path, damping_factor=damping_factor)

        print_top_pages(standard, "d", damping_factor)
        results.append((damping_factor, standard, map_reduce, max_difference))

    print_experiment_table(results, "Damping")