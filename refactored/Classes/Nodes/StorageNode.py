import threading
import zmq

from pathlib import Path

import Classes.Nodes.messages_pb2 as messages_pb2


class StorageNode:
    """
    Encapsulate the features of a storage node, running on a thread
    """

    def __init__(self, name: str, push_port: int, pull_port: int, sub_port: int, context: zmq.Context):

        self.name = name

        self.push_port = push_port
        self.pull_port = pull_port
        self.sub_port = sub_port

        self.context = context

        self.folder = Path(__file__).resolve().parent / "nodes" / name
        self.folder.mkdir(parents = True, exist_ok = True)

        self.stop_event = threading.Event()

        self.thread = threading.Thread(
            target=self.thread_function,
            name=self.name   
        )


    """ ==============  THREADING  ============== """

    def thread_function(self):

        ip = lambda port: f"tcp://localhost:{port}"

        subscriber = self.context.socket(zmq.SUB)
        receiver = self.context.socket(zmq.PULL)
        sender = self.context.socket(zmq.PUSH)

        poller = zmq.Poller()

        try:

            subscriber.connect(ip(self.sub_port))
            subscriber.setsockopt(zmq.SUBSCRIBE, b"")
            subscriber.setsockopt(zmq.LINGER, 0)

            receiver.connect(ip(self.pull_port))
            receiver.setsockopt(zmq.LINGER, 0)

            sender.connect(ip(self.push_port))
            sender.setsockopt(zmq.LINGER, 0)

            poller.register(subscriber, zmq.POLLIN)
            poller.register(receiver, zmq.POLLIN)

            while not self.stop_event.is_set():

                sockets = dict(poller.poll(timeout=1000))

                if receiver in sockets:
                    self.__receiver_response(receiver, sender)

                if subscriber in sockets:
                    self.__subscriber_response(subscriber, sender)

        except zmq.ZMQError as e:
            if not self.stop_event.is_set():
                print(f"{self.name}: ZeroMQ error: {e}")

        finally:
            subscriber.close()
            receiver.close()
            sender.close()

        
    def start(self):
        """
        Start the thread
        """
        self.thread.start()


    def stop(self):
        """
        Stop the thread
        """
        self.stop_event.set()

        if self.thread.is_alive():
            self.thread.join()


    def is_alive(self) -> bool:
        """
        Check the thread is alive
        """
        return self.thread.is_alive()


    """ ==============  FILE MANAGEMENT  ============== """

    def __write_file(self, filename: str, data: bytes) -> bool:
        try:

            with open(self.folder / filename, 'wb') as file:
                file.write(data)

            return True
        
        except EnvironmentError as e:
            print("Error writing file: {}".format(e))
            return False


    """ ==============  ZMQ  ============== """

    def __subscriber_response(self, subscriber: zmq.Socket, sender: zmq.Socket):
        """
        The response when a file request is published - send the file if exists

        :param subscriber: A socket subscribed
        :param sender: A 
        """

        msg = subscriber.recv()

        task = messages_pb2.GetDataRequest()
        task.ParseFromString(msg)

        filename = task.filename

        try:
            with open(self.folder / filename, "rb") as file:
                sender.send_multipart([
                    bytes(filename, 'utf-8'),
                    file.read()
                ])
        except FileNotFoundError:
            pass


    def __receiver_response(self, receiver: zmq.Socket, sender: zmq.Socket):
        """
        The response when receiving 
        """

        msg = receiver.recv_multipart()

        task = messages_pb2.StoreDataRequest()
        task.ParseFromString(msg[0])

        filename = task.filename
        data = msg[1]

        if self.__write_file(filename, data):
            sender.send(filename.encode('utf-8'))
        else:
            sender.send("error".encode('utf-8'))

    """ ==============  Utility  ============== """

    def get_name(self) -> str:
        return self.name

    def get_receiver_port(self) -> int:
        return self.pull_port

if __name__ == "__main__":

    context = zmq.Context()

    default_sub_port = 5000
    default_push_port = 5001
    default_pull_port = 5002

    nodes = []

    for node_number in range(1,10):
        nodes.append(
            StorageNode(
                f"node-0{node_number}" if node_number < 10 else f"node-{node_number}",
                default_push_port,
                default_pull_port,
                default_sub_port,
                context
            )
        )
        
    for node in nodes:
        print(node.get_name())
