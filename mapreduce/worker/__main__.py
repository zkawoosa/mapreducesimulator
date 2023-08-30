"""MapReduce framework Worker node."""
import os
import hashlib
import logging
import json
import time
import threading
import shutil
import socket
import subprocess
import tempfile
import heapq
from contextlib import ExitStack
from pathlib import Path
import click
# Configure logging
LOGGER = logging.getLogger(__name__)
class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )
        self.signals={"shutdown":False}
        self.worker_host = host
        self.worker_port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.threads = []
        self.current_message = {}
        self.message_dict ={"friend": 1}
        # self.UDP_thread = threading.Thread() # target = heartbeat
        # self.threads.append(self.UDP_thread)
        # self.UDP_thread.start()

        self.worker_thread = threading.Thread(target=self.start_server, args=(host,port))
        self.threads.append(self.worker_thread)
        self.worker_thread.start()
        self.register()
        for thread in self.threads:
            thread.join()
        # This is a fake message to demonstrate pretty printing with logging
        # message_dict = {
        #     "message_type": "register_ack",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        # }
        # LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))
        # exit immediately!
        # LOGGER.debug("IMPLEMENT ME!")
        # time.sleep(120)

    def send_message(self):
        """Pylint error handling comment."""
        LOGGER.info("Sending message")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            job_byte = json.dumps(self.current_message).encode('utf-8')
            sock.sendall(job_byte)

    def register(self):
        """Pylint error handling comment."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            registration_message = {
                    "message_type" : "register",
                    "worker_host" : self.worker_host,
                    "worker_port" : self.worker_port
                    }
            sock.connect((self.manager_host, self.manager_port))
            # send a message
            message = json.dumps(registration_message).encode('utf-8')
            sock.sendall(message)
    def heartbeat(self):
        """Pylint error handling comment."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            while not self.signals["shutdown"]:
                heart_beat_message = {
                    "message_type": "heartbeat",
                    "worker_host": self.worker_host,
                    "worker_port": self.worker_port
                }
                sock.connect((self.manager_host, self.manager_port)) # the problem is here
                # Send a message
                message = json.dumps(heart_beat_message)
                sock.sendall(message.encode('utf-8'))
                time.sleep(2)
    def map_thread(self):
        """Pylint error handling comment."""
        LOGGER.info("Running map thread")
        prefix = f"mapreduce-local-task{self.message_dict['task_id']:05d}-"
        with ExitStack() as exit_stack:
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                #LOGGER.info("Opened tmpdir")
                partition_paths = [os.path.join(tmpdir,
                        f"maptask{self.message_dict['task_id']:05d}-part{i:05d}") 
                        for i in range(self.message_dict['num_partitions'])]
                partition_files = [open(path, 'a', encoding='utf-8')
                                for path in partition_paths]
                for inputfile in self.message_dict['input_paths']:
                    if self.signals['shutdown']:
                        return
                    # Run executable on each file, write each line to hashed destination
                    with open(Path(inputfile), encoding='utf-8') as infile:
                        LOGGER.info("Opened input path")
                        with subprocess.Popen(
                            [self.message_dict['executable']],
                            stdin=infile,
                            stdout=subprocess.PIPE,
                            text=True,
                        ) as map_process:
                            for line in map_process.stdout:
                                hexdigest = hashlib.md5(
                                    line.split('\t')[0].encode("utf-8")).hexdigest()
                                # Check if files exists, if not create
                                partition_files[int(hexdigest, base=16) %
                                self.message_dict['num_partitions']].write(line)

                for partition_file in partition_files:
                    partition_file.close()

                for partition_path in partition_paths:
                    unsorted_part = [line for line in
                                     exit_stack.enter_context(open(partition_path, "r"
                                    , encoding='utf-8'))]
                    unsorted_part.sort()
                    LOGGER.info("Sorted partition")
                    sorted_file = exit_stack.enter_context(open(partition_path, "w"
                                                        , encoding='utf-8'))
                    sorted_file.writelines(unsorted_part)
                    LOGGER.info("Wrote to sorted_part")    
                    # with open(partition_path, "r") as unsorted_file:
                    #     unsorted_part = unsorted_file.readlines()
                    #     unsorted_part.sort()
                    #     LOGGER.info("Sorted partition")
                    # with open(partition_path, "w") as sorted_file:
                    #     sorted_file.writelines(unsorted_part)
                    #     LOGGER.info("Wrote to sorted_part")
                    shutil.move(partition_path, self.message_dict['output_directory'])
                LOGGER.info("exited with statement")
                self.current_message = {
                    "message_type": "finished",
                    "task_id": self.message_dict['task_id'],
                    "worker_host": self.worker_host,
                    "worker_port": self.worker_port
                }
                self.send_message()
    def reduce_thread(self):
        """Pylint error handling comment."""
        executable = self.message_dict['executable']
        input_paths = self.message_dict['input_paths']
        task_id = self.message_dict['task_id']
        output_directory = self.message_dict['output_directory']
        prefix = f"mapreduce-local-task{task_id:05d}-"
        with ExitStack() as stack:
            # for input_path in input_paths:
            #     files.append()
            input_files = [stack.enter_context(open(fname, encoding='utf-8'))
                           for fname in input_paths]
            iterator = heapq.merge(*input_files)
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                # LOGGER.info(f"Opened tmpdir")
                path = os.path.join(tmpdir,
                    f"part-{task_id:05d}")
                    # Run executable on each file, write each line to hashed destination
                LOGGER.info(path)
                with open(path,'w', encoding='utf-8') as out_file:
                    LOGGER.info("Opened input path")
                    with subprocess.Popen(
                        [executable],
                        stdin=subprocess.PIPE,
                        stdout=out_file,
                        text=True,
                    ) as reduce_process:
                        for line in iterator:
                            reduce_process.stdin.write(line)
                shutil.move(path,output_directory)
            self.current_message = {
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.worker_host,
                "worker_port": self.worker_port
            }
            self.send_message()

    def msg(self,message_chunks):
        """Pylint error handling comment."""
        message_bytes = b''.join(message_chunks)
        message_str = message_bytes.decode("utf-8")
        return message_str
    def start_server(self,host,port):
        """Pylint error handling comment."""
        LOGGER.info("created start server for worker")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host,port))
            sock.listen()
            sock.settimeout(1)
            while not self.signals["shutdown"]:
                LOGGER.info("W not shut down")
                try:
                    clientsocket = sock.accept()[0]
                except:
                    continue
                clientsocket.settimeout(1)
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                message_str = self.msg(message_chunks)
                try:
                    message_dict = json.loads(message_str)
                    if message_dict["message_type"] == "shutdown":
                        self.signals['shutdown'] = True
                    elif message_dict["message_type"] == "register_ack":
                        udp_thread = threading.Thread(
                            target=self.heartbeat
                        ) # target = heartbeat
                        self.threads.append(udp_thread)
                        udp_thread.start()
                    elif message_dict["message_type"] == "new_map_task":
                        self.message_dict = message_dict
                        map_worker_thread = threading.Thread(target = self.map_thread)
                        self.threads.append(map_worker_thread)
                        map_worker_thread.start()
                        map_worker_thread.join()
                    elif message_dict["message_type"] == "new_reduce_task":
                        self.message_dict = message_dict
                        reduce_worker_thread = threading.Thread(target = self.reduce_thread)
                        self.threads.append(reduce_worker_thread)
                        reduce_worker_thread.start()
                        reduce_worker_thread.join()
                    else:
                        LOGGER.error("Bad worker command")
                except json.JSONDecodeError:
                    continue
@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)

if __name__ == "__main__":
    main()
    