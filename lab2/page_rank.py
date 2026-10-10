from config import DAMPING_FACTOR, EPSILON, MAX_ITERATIONS, DEBUG, DEBUG_SAMPLE_SIZE


class PageRank:

    def __init__(self, damping_factor=DAMPING_FACTOR, epsilon=EPSILON, max_iterations=MAX_ITERATIONS, debug=DEBUG):
        self.damping_factor = damping_factor
        self.epsilon = epsilon
        self.max_iterations = max_iterations
        self.debug = debug

    @staticmethod
    def is_valid_line(line):
        return bool(line.strip()) and not line.startswith("#")

    @staticmethod
    def parse_line(line):
        source, target = line.split()
        return source, target

    @staticmethod
    def get_rank(item):
        return item[1]

    @classmethod
    def read_edges(cls, spark_context, input_path):
        return spark_context.textFile(input_path).filter(cls.is_valid_line).map(cls.parse_line).cache()

    @staticmethod
    def collect_nodes(edges):
        return edges.flatMap(PageRank.edge_nodes).distinct()

    @staticmethod
    def edge_nodes(edge):
        return edge

    def apply_formula(self, contribution_sum):
        return (1.0 - self.damping_factor) + self.damping_factor * contribution_sum

    def print_stage(self, message):
        if self.debug:
            print(message)

    def print_data(self, title, data):
        if not self.debug:
            return

        if hasattr(data, "take"):
            sample = data.take(DEBUG_SAMPLE_SIZE)
        elif isinstance(data, dict):
            sample = list(data.items())[:DEBUG_SAMPLE_SIZE]
        else:
            sample = list(data)[:DEBUG_SAMPLE_SIZE]

        print(f"  {title} (first {DEBUG_SAMPLE_SIZE}):")

        for item in sorted(sample):
            print(f"    {item}")

    def print_iteration(self, iteration, delta):
        self.print_stage(f"Iteration {iteration}: delta = {delta:.8f}")

    @staticmethod
    def build_graph(nodes, edges):
        incoming = {node: [] for node in nodes}
        outgoing = {node: 0 for node in nodes}

        for source, target in edges:
            incoming[target].append(source)
            outgoing[source] += 1

        return incoming, outgoing

    def calculate_node_rank(self, node, incoming, outgoing, ranks):
        contribution_sum = sum(ranks[source] / outgoing[source] for source in incoming[node])
        return self.apply_formula(contribution_sum)

    def run(self, spark_context, input_path):
        self.print_stage("Stage 1: reading edges")
        edges = self.read_edges(spark_context, input_path)

        try:
            nodes = self.collect_nodes(edges).collect()
            edge_list = edges.collect()

            self.print_data("edges (source, target)", edge_list)

            self.print_stage(f"Stage 2: building the graph ({len(nodes)} pages, {len(edge_list)} links)")

            incoming, outgoing = self.build_graph(nodes, edge_list)

            self.print_data("incoming pages (page, pages that link to it)", incoming)
            self.print_data("outgoing links (page, number of links)", outgoing)

            self.print_stage("Stage 3: initializing ranks")

            ranks = {node: 1.0 for node in nodes}
            self.print_data("initial ranks (page, rank)", ranks)

            threshold = self.epsilon * len(nodes)
            iteration = 0
            delta = float("inf")

            self.print_stage("Stage 4: iterating until the ranks stop changing")

            while delta >= threshold and iteration < self.max_iterations:
                new_ranks = {node: self.calculate_node_rank(node, incoming, outgoing, ranks) for node in nodes}
                delta = sum(abs(new_ranks[node] - ranks[node]) for node in nodes)

                ranks = new_ranks
                iteration += 1

                self.print_iteration(iteration, delta)
                self.print_data("new ranks (page, rank)", ranks)

            self.print_stage("Stage 5: sorting the result")

            result = sorted(ranks.items(), key=self.get_rank, reverse=True)
            return result, iteration, delta

        finally:
            edges.unpersist()