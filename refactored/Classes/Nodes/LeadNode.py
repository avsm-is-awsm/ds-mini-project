import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from Classes.Database.Database import Database
from Classes.Nodes.StorageNode import StorageNode
import Classes.Nodes.messages_pb2 as messages_pb2

from Util.Util import parse_name

import zmq


from random import sample, SystemRandom
from string import ascii_letters, digits
from time import sleep
from typing import Any


class LeadNode:
    """
    Encapsulate the features of the lead node
    """

    def __init__(self, pull_port: int, publish_port: int, push_port: int, number_of_nodes: int, context: zmq.Context, RESET_DATABASE: bool = False):
        """
        Setup the lead node and create the specified number of storage nodes

        :param int push_port:  The starting push port of the lead node, each storage node will receive its own pull node
        :param int pull_port: The pull port of the lead node
        :param int publish_port: The publish port
        :param int number_of_nodes: The number of storage node in the system
        :param zmq.Context context: The zmq context used to setup all sockets
        """

        self.context = context

        self.database = Database(RESET_DATABASE)

        self.push_port = push_port
        self.pull_port = pull_port
        self.publish_port = publish_port

        self.publisher = context.socket(zmq.PUB)
        self.receiver = context.socket(zmq.PULL)

        self.nodes = self.__setup_storage_nodes(number_of_nodes)
        self.senders = self.__setup_lead_node()

        self.poller = zmq.Poller()
        self.poller.register(self.receiver, zmq.POLLIN)

        self.STARTED = False
    

    """ ==============  APPLICATION  ============== """

    def store_file(self, filename: str, data: bytes, content_type: str, mode: str) -> int:
        """
        Store a file in the distributed storage system

        :param str filename: The name of the file
        :param bytes data: The data of the file
        :param str content_type: The type of file - a parameter from flask requests
        :param str mode: The mode used: _full_, _compressed_

        :return result: Whether the file was stored succesfully
        """

        name, version = parse_name(filename)
        newest_database_entry = self.__get_newest_database_version(name)

        nodes = self.__get_random_nodes(3)

        match mode.lower():
            case "compression":
                return self.__store_compressed_files(
                    name = name,
                    data = data,
                    content_type = content_type,
                    version = version,
                    nodes = nodes,
                    mode = mode
                )

            case "full":
                return self.__store_full_files(
                    name = name,
                    data = data,
                    content_type = content_type,
                    version = version,
                    nodes = nodes,
                    mode = mode
                )

            case _:
                return self.__store_full_files(
                    name = name,
                    data = data,
                    content_type = content_type,
                    version = version,
                    nodes = nodes,
                    mode = mode
                )


    def retrieve_file(self, filename: str) -> tuple[(bytes | None), (dict[str, str | int] | None)]:
        """
        retrieve the the requested file from StorageNodes

        :param filename: The file to retrieve from the files

        :return filedata: The contents of the requested file and the corresponding database entry
        """

        name, version = parse_name(filename)
        newest_database_entry = self.__get_newest_database_version(name)

        if (not newest_database_entry) or (newest_database_entry["version"] != version):
            return (None, newest_database_entry)

        mode = self.__get_storage_mode(newest_database_entry)

        match mode.lower():
            case "compression":
                return self.__retrieve_compressed_file(
                    database_entry = newest_database_entry
                )

            case "full":
                return self.__retrieve_full_file(
                    database_entry = newest_database_entry
                )

            case _:
                return self.__retrieve_full_file(
                    database_entry = newest_database_entry
                )


    def start(self):
        """
        Start the StorageNodes if they are not running already
        """

        if not self.STARTED:
            for node in self.nodes:
                node.start()
            self.STARTED = True

            # give time for all threads to start
            sleep(2)


    def stop(self):
        """
        Stop all StorageNodes and LeadNode
        """
        
        for node in self.nodes:
            node.stop()

        for sender in self.senders.values():
            sender.close()

        self.receiver.close()
        self.publisher.close()

        self.database.close()

        self.STARTED = False


    """ ==============  ZMQ  ============== """

    def __store_full_files(self, name: str, data: bytes, content_type: str, version: int, nodes: list[StorageNode], mode: str) -> int:
        """
        Storing a full replicas on nodes and storing the entries into the database

        :param str name:                    The full parsed name of the file e.g. 'apple'
        :param bytes data:                  The data of the file in bytes
        :param str content_type:            The type of file
        :param int version:                 The version of the file
        :param list[StorageNode] nodes:     The nodes to store the copies on
        :param str mode:                    The selected mode, added to storage details
        
        """

        blob_names = [self.__generate_blob_name(12) for _ in range(3)]

        storage_details = {
            "mode": mode,
            "names": blob_names
        }

        task = messages_pb2.StoreDataRequest()

        failed_attempts = 0
        for index, node in enumerate(nodes):
            task.filename = blob_names[index]
            self.senders[node.get_name()].send_multipart([
                task.SerializeToString(),
                data
            ])

            reply = self.receiver.recv()

            if reply.decode("utf-8") != blob_names[index]:
                failed_attempts += 1

        if failed_attempts == 3:
            return 404

        self.database.insert_file(
            filename = name,
            size = len(data),
            content_type = content_type,
            storage_details = storage_details,
            version = version
        )

        return 201


    def __retrieve_full_file(self, database_entry: dict) -> tuple[Any, dict]:

        blob_names = database_entry["storage_details"]["names"]

        for blob_name in blob_names:

            try:
                data = self.__request_file(name = blob_name)

                if data is not None:
                    return (data, database_entry)

            except zmq.ZMQError:
                continue

        return (None, database_entry)


    def __store_compressed_files(self, name: str, data: bytes, content_type: str, version: int, nodes: list[StorageNode], mode: str) -> bool:
        return True


    def __retrieve_compressed_file(self, database_entry: dict) -> tuple[bytes, dict]:
        return bytes(), database_entry


    def __request_file(self, name: str) -> bytes | str | None:
        """
        Request a file using the publish socket

        :param str name: The name stored on the StorageNodes
        
        :return data: The data of the file
        """

        task = messages_pb2.GetDataRequest()
        task.filename = name

        self.publisher.send(
            task.SerializeToString()
        )

        events = dict(self.poller.poll(1000))
        if self.receiver not in events:
            return None

        result = self.receiver.recv_multipart()
        name_received = result[0].decode('utf-8')

        if (len(result) != 2):
            return None

        if (name != name_received):
            return None

        if (not result[1]):
            return None

        return result[1]


    """ ==============  UTILITY  ============== """


    def __get_newest_database_version(self, filename: str) -> dict[str, str | int] | None:
        """
        Select all files with that name, find the newest version
        """

        files = self.database.select_one_file(filename)

        if not files:
            return None

        newest_file = files[0]
        for file in files[1:]:
            if file["version"] > newest_file["version"]:
                newest_file = file

        return newest_file


    def __get_storage_mode(self, database_entry: dict) -> str:
        """
        Check which storage mode was used  for the given file

        :param dict database_entry: The database entry parsed from JSON to a dictionary

        :return mode: The mode used
        """

        return database_entry["storage_details"]["mode"]


    def __get_random_nodes(self, number_of_nodes: int) -> list[StorageNode]:
        """
        Get a random sample of nodes

        :param int number_of_nodes: The number of nodes in the sample

        :return sample: A sample of StorageNodes containing _number_of_nodes_
        """
        return sample(
            self.nodes,
            number_of_nodes    
        )

    @staticmethod
    def __generate_blob_name(length: int):
        """
        Returns a random alphanumeric string of the given length. 
        Only lowercase ascii letters and numbers are used.

        :param length: Length of the requested random string 
        :return: The random generated string
        """
        return ''.join([SystemRandom().choice(ascii_letters + digits) for n in range(length)]) 
      

    """ ==============  INITIALISATION  ============== """

    def __setup_storage_nodes(self, number_of_nodes: int) -> list[StorageNode]:
            """
            Create a list of StorageNodes using the port configuration given to the LeadNode

            :param int number_of_nodes: The number of StorageNodes created

            :return nodes: A list of StorageNode objects
            """

            nodes = []

            for node_number in range(number_of_nodes):

                nodes.append( 
                    StorageNode(
                        name = f"StorageNode-0{node_number}" if node_number < 10 else \
                                f"StorageNode-{node_number}",
                        push_port = self.pull_port,
                        pull_port = self.push_port + node_number,
                        sub_port = self.publish_port,
                        context = self.context 
                    )
                )
                
            return nodes


    def __setup_lead_node(self) -> dict[str, zmq.Socket]:
        """
        Setup the push ports and connect them to each corresponding StorageNode

        :return senders: A dictionary mapping the StorageNode name to corresponding the LeadNode push socket 
        """

        self.publisher.bind(f"tcp://localhost:{self.publish_port}")
        self.publisher.setsockopt(zmq.LINGER, 0)

        self.receiver.bind(f"tcp://localhost:{self.pull_port}")
        self.receiver.setsockopt(zmq.LINGER, 0)

        senders = {}

        for node in self.nodes:

            socket = self.context.socket(zmq.PUSH)
            socket.bind(f"tcp://localhost:{node.get_receiver_port()}")

            senders[node.get_name()] = socket

        return senders


if __name__ == "__main__":

    context = zmq.Context()

    lead_node = LeadNode(
        5000,
        5001,
        5002,
        10,
        context
    )

    try:

        lead_node.start()

        while True:
            for node in lead_node.nodes:
                print(f"Is {node.get_name()} alive? {node.is_alive()}")

            sleep(1)

    except KeyboardInterrupt:
        pass

    finally:
        lead_node.stop()
