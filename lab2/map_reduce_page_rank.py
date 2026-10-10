from page_rank import PageRank
from config import DAMPING_FACTOR, EPSILON, MAX_ITERATIONS, DEBUG


class MapReducePageRank(PageRank):

    def __init__(self, damping_factor=DAMPING_FACTOR, epsilon=EPSILON, max_iterations=MAX_ITERATIONS, debug=DEBUG, partitions=None):
        super().__init__(damping_factor, epsilon, max_iterations, debug)
        self.partitions = partitions

    @staticmethod
    def create_link(edge):
        return edge[0], [edge[1]]

    @staticmethod
    def create_empty_link(node):
        return node, []

    @staticmethod
    def merge_links(first, second):
        return first + second

    @staticmethod
    def initialize_rank(node):
        return node, 1.0

    @staticmethod
    def distribute_rank(page_data):
        node, (links, rank) = page_data
        contributions = [(node, 0.0)]

        if links:
            contribution = rank / len(links)

            for link in links:
                contributions.append((link, contribution))

        return contributions

    @staticmethod
    def sum_contributions(first, second):
        return first + second

    @staticmethod
    def prepare_delta(item):
        old_rank, new_rank = item[1]
        return abs(new_rank - old_rank)

    def print_iteration_data(self, page_data, contributions, contribution_sums, new_ranks):
        if not self.debug:
            return

        self.print_data("join: (page, (outgoing links, rank))", page_data)
        self.print_data("MAP: (target page, contribution)", contributions)
        self.print_data("REDUCE: (page, sum of contributions)", contribution_sums)
        self.print_data("formula: (page, new rank)", new_ranks)

    def run(self, spark_context, input_path):
        self.print_stage("Stage 1: reading edges")
        edges = self.read_edges(spark_context, input_path)

        partitions = self.partitions or spark_context.defaultParallelism
        nodes = self.collect_nodes(edges).cache()
        links = nodes.map(self.create_empty_link).union(edges.map(self.create_link)).reduceByKey(self.merge_links, partitions).cache()

        ranks = None

        try:
            self.print_data("edges (source, target)", edges)

            node_count = nodes.count()
            edge_count = edges.count()

            self.print_stage(f"Stage 2 (map + reduce): building links ({node_count} pages, {edge_count} links)")
            self.print_data("links (page, outgoing links)", links)

            self.print_stage("Stage 3: initializing ranks")

            ranks = nodes.map(self.initialize_rank).cache()
            self.print_data("initial ranks (page, rank)", ranks)

            threshold = self.epsilon * node_count
            iteration = 0
            delta = float("inf")

            self.print_stage("Stage 4 (map + reduce): iterating until the ranks stop changing")

            while delta >= threshold and iteration < self.max_iterations:
                page_data = links.join(ranks, partitions)
                contributions = page_data.flatMap(self.distribute_rank)
                contribution_sums = contributions.reduceByKey(self.sum_contributions, partitions)
                new_ranks = contribution_sums.mapValues(self.apply_formula).cache()

                delta = ranks.join(new_ranks, partitions).map(self.prepare_delta).sum()

                iteration += 1

                self.print_iteration(iteration, delta)
                self.print_iteration_data(page_data, contributions, contribution_sums, new_ranks)

                ranks.unpersist()
                ranks = new_ranks

            self.print_stage("Stage 5: sorting the result")

            result = ranks.sortBy(self.get_rank, ascending=False).collect()
            return result, iteration, delta

        finally:
            if ranks is not None:
                ranks.unpersist()

            links.unpersist()
            nodes.unpersist()
            edges.unpersist()