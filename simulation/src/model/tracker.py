from src.utils.definitions import GB


class Tracker:
    def __init__(self):
        self.latency_data = {"read": [], "write": []}
        self.tput_runtime_data = {"read": [], "write": []}
        self.throughput_data = {"read": [], "write": []}
        self.transfer_costs = []
        self.storage_costs = []
        self.storage_costs_without_base = []
        self.request_costs = []
        self.request_sizes = []
        self.total_cost = 0

        self.num_req = 0
        self.round_digit = 9
        self.duration = 0
        self.days_to_ignore = 0

    def set_duration(self, duration):
        self.duration = duration

    def add_throughput(self, operation, throughput):
        self.throughput_data[operation].append(throughput)

    def set_day_to_ignore(self, days_to_ignore):
        self.days_to_ignore = days_to_ignore

    def add_tput_runtime(self, operation, tput_runtime):
        self.tput_runtime_data[operation].append(tput_runtime)

    def add_latency(self, operation, latency):
        self.latency_data[operation].append(latency)

    def add_transfer_cost(self, cost):
        self.transfer_costs.append(cost)

    def add_storage_cost(self, cost):
        self.storage_costs.append(cost)

    def add_storage_cost_without_base(self, cost):
        self.storage_costs_without_base.append(cost)

    def add_request_cost(self, cost):
        self.request_costs.append(cost)

    def add_request_size(self, size):
        self.request_sizes.append(size)
        self.num_req += 1

    def compute_average(self, data_list):
        return round(sum(data_list) / len(data_list), 4) if data_list else 0

    def compute_sum(self, data_list):
        return round(sum(data_list), self.round_digit) if data_list else 0

    def get_metrics(self):
        tot_transfer_cost = self.compute_sum(self.transfer_costs)
        tot_storage_cost = self.compute_sum(self.storage_costs)
        tot_storage_cost_without_base = self.compute_sum(
            self.storage_costs_without_base
        )
        tot_request_cost = self.compute_sum(self.request_costs)
        self.total_cost = tot_transfer_cost + tot_storage_cost + tot_request_cost

        metrics = {
            "Trace Duration (s)": self.duration,
            "Cost Calculated Since Day": self.days_to_ignore,
            "total requests": self.num_req,
            "avg request size (GB)": self.compute_average(self.request_sizes) / GB,
            "aggregate size (GB)": self.compute_sum(self.request_sizes) / GB,
            "avg read latency (ms)": self.compute_average(self.latency_data["read"]),
            "avg write latency (ms)": self.compute_average(self.latency_data["write"]),
            "avg read tput runtime (ms)": self.compute_average(
                self.tput_runtime_data["read"]
            )
            * 1000,
            "avg write tput runtime (ms)": self.compute_average(
                self.tput_runtime_data["write"]
            )
            * 1000,
            "avg read throughput (Gbps)": self.compute_average(
                self.throughput_data["read"]
            ),
            "avg write throughput (Gbps)": self.compute_average(
                self.throughput_data["write"]
            ),
            "total transfer cost ($)": tot_transfer_cost,
            "total storage cost ($)": tot_storage_cost,
            "total storage cost without base ($)": tot_storage_cost_without_base,
            "total request cost ($)": tot_request_cost,
            "total cost ($)": round(self.total_cost, self.round_digit),
        }
        return metrics

    def get_detailed_metrics(self):
        return {
            "read_latencies": self.latency_data["read"],
            "write_latencies": self.latency_data["write"],
            "transfer_costs": self.transfer_costs,
            "storage_costs": self.storage_costs,
            "storage_costs_without_base": self.storage_costs_without_base,
            "request_costs": self.request_costs,
            "total_cost": round(self.total_cost, self.round_digit),
        }
