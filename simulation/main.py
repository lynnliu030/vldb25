from src.simulator import Simulator
from src.simulator_v2 import SimulatorV2
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SkyStorage Simulator")
    parser.add_argument(
        "--config", default="config/spanstore.yaml", help="Path to config file"
    )
    parser.add_argument(
        "--trace",
        default="traces/mc_dir/fixed/IBMObjectStoreTrace003Part0.typeA.mc",
        help="Path to trace file",
    )
    parser.add_argument("--vm", default=5, help="Number of VMs")
    # add a bool variable to set base region
    parser.add_argument("--setbase", default=False, help="Set base region")
    parser.add_argument(
        "--days", default=0, help="Only calculate cost after Day [days]"
    )
    parser.add_argument("--simversion", default="0", help="Simulator version")
    args = parser.parse_args()
    if args.simversion == "0":
        simulator = Simulator(
            args.config,
            args.trace,
            int(args.vm),
            bool(args.setbase),
            int(args.days),
            version_enable=True,
        )
    else:
        simulator = SimulatorV2(
            args.config,
            args.trace,
            int(args.vm),
            bool(args.setbase),
            int(args.days),
            version_enable=True,
        )  # , store_decision = True
    simulator.run()

    # simulator.plot_graphs(graphs)
    simulator.report_metrics()
