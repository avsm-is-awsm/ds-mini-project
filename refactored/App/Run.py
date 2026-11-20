import zmq

import argparse
import sys
import subprocess

from App.App import create_app
from App.Config import SERVER_IP, SERVER_PORT
from Classes.Nodes.LeadNode import LeadNode
from Util.Util import wait_for_app_process

from time import sleep

def run_app(RESET_DATABASE: bool = False, number_of_storage_nodes: int = 6):

    context = zmq.Context()

    lead_node = LeadNode(
        pull_port = 5000,
        publish_port = 5001,
        push_port = 5002,
        number_of_nodes = number_of_storage_nodes,
        context = context,
        RESET_DATABASE = RESET_DATABASE
    )

    app = create_app(lead_node)

    try:

        lead_node.start()

        # wait for all nodes to launch
        while not all(storage_node.is_alive() for storage_node in lead_node.nodes):
            sleep(0.1)

        app.run(
            host=SERVER_IP, 
            port=SERVER_PORT,
            use_reloader = False
        )
    
    finally:
        lead_node.stop()
        context.term()


def run_app_on_process(RESET_DATABASE: bool, number_of_storage_nodes: int = 6) -> subprocess.Popen:
    """
    Run the app as a process

    :param bool RESET_DATABASE: reset the sqlite3 database when launching the process
    :param int number_of_storage_nodes: The number of storage nodes in the distributed system

    :return proces: The distributed system running as a subprocess
    """

    system = subprocess.Popen(
        [
        sys.executable,
        "-m",
        "App.Run",
        "--reset-database",
        "--storage-nodes",
        str(number_of_storage_nodes)
        ],
        stdout = subprocess.DEVNULL,    
        stderr = subprocess.DEVNULL     # remove for debug
    )

    wait_for_app_process(
        process = system,
        timeout = 10 
    )

    return system


def parse() -> argparse.Namespace:
    """
    Need to pass arguments to be able to launch the app as a configurable process

    :return arguments: Arguments passed to the run.py file
    """

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--reset-database",
        action="store_true"
    )

    parser.add_argument(
        "--storage-nodes",
        type=int,
        default=6
    )


    args = parser.parse_args()

    if args.storage_nodes < 3:
        parser.error("At least 3 storage nodes are required")

    return parser.parse_args()


if __name__ == "__main__":

    args = parse()

    run_app(
        RESET_DATABASE = args.reset_database,
        number_of_storage_nodes = args.storage_nodes
    )