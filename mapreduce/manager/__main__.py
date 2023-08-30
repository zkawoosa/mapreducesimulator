"""MapReduce framework Manager node."""
import os
import tempfile
import threading
import logging
import json
import socket
import time
import queue
import copy
from pathlib import Path
import shutil
import click
from mapreduce.manager.__shutdown__ import shut_down

# Configure logging
LOGGER = logging.getLogger(__name__)

class Task:
    """Pylint error handling comment."""

    def __init__(self, message):
        """Pylint error handling comment."""
        self.message = message
        self.worker_id = None
        self.is_completed = False
        #  self.is_assigned = False
        self.needs_reassign = False
    def ret(self):
        """Pylint error handling comment."""
        return self.message


class ReduceTask:
    """Pylint error handling comment."""

    def __init__(self, file_list):
        """Pylint error handling comment."""
        self.file_list = file_list
        self.worker_id = None
        self.is_completed = False
        #  self.is_assigned = False
        self.needs_reassign = False
    def file(self):
        """Pylint error handling comment."""
        return self.file_list


class WorkerRep:
    """Represents a worker with relevant information."""

    def __init__(self, host, port, id, last_time):
        """Pylint error handling comment."""
        self.host = host
        self.port = port
        self.id = id
        self.status = "ready"
        self.last_time = last_time
        self.task_id = -1
    def send_msg(self, msg):
        """Pylint error handling comment."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))
            msg_bytes = json.dumps(msg).encode('utf-8')
            sock.sendall(msg_bytes)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        self.host = host
        self.port = port
        self.workers = []
        #  Queue will contain
        self.job_queue = queue.Queue()
        #  For new job request
        self.job_id = 0
        #  bool if we're working or not
        self.is_working_on_job = False
        #  Only one job can execute at a time, track if one is
        self.job_running = False
        #  Sets up UDP hearbeat thread and saves into a list
        self.threads = []
        self.udp = threading.Thread(target=self.heartbeat)
        self.udp_running = False
        #  Map worker host port to their id
        self.hp_id = {}
        self.signals = {"shutdown": False}
        #  self.threads.append(self.cid_thread)
        #  Task type for message finished interpretation
        self.is_mapping = False
        #  current job's dictionary from queue.get()
        self.curr_job = {}
        #  running total of assigned and completed tasks for each job
        self.tasks_completed = 0
        #  For each task completed = True
        #  self.task_status = {}
        #  task-id to task dictionary for mapping
        self.tasks = {}
        #  task-id to reduceTask dictionary for reducing
        self.reduce_tasks = {}
        #  self.job_tmp_dir = ""
        #  add threads for things that have to happen all the time (ie run job)
        #  sets up PDP listening socket
        #  allows the threads to run, once they are done running we're done
        self.m_t = threading.Thread(target=self.start_server, args=(host, port))
        self.threads.append(self.m_t)
        self.m_t.start()
        self.rtj_thread = threading.Thread(target=self.run_the_jobs)
        #  self.threads.append(self.run_job_thread)
        #  self.threads.append(self.rtj_thread)
        self.threads.append(self.rtj_thread)
        self.rtj_thread.start()
        #  thread that loops and checks if workers are dead
        self.cid_thread = threading.Thread(target=self.check_if_dead)
        self.cid_thread.start()
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        for thread in self.threads:
            thread.join()

    def check_if_dead(self):
        """Pylint error handling comment."""
        while not self.signals['shutdown']:
            #  LOGGER.info("checking for workers")
            for worker in self.workers:
                if worker.status != 'dead':
                    if (time.time() - worker.last_time) > 10:
                        if worker.status == "busy":
                            task_id = worker.task_id
                            if self.is_mapping:
                                self.tasks[task_id].needs_reassign = True
                            else:
                                self.reduce_tasks[task_id].needs_reassign = True
                        worker.status = 'dead'
            time.sleep(0.1)

    def heartbeat(self):
        """Pylint error handling comment."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            #  Read message and update time for given worker
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            #  sock.bind(("localhost", 8001))
            sock.settimeout(1)
            while not self.signals['shutdown']:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                if message_dict['message_type'] == 'heartbeat':
                    worker_port = message_dict['worker_port']
                    if worker_port in self.hp_id:
                        current_worker = self.hp_id[worker_port]
                        self.workers[current_worker].last_time = time.time()
                time.sleep(1)

    def start_server(self, host, port):
        """Pylint error handling comment."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
            sock.settimeout(1)
            while not self.signals["shutdown"]:
                #  LOGGER.info(f"M not shut down")
                try:
                    clientsocket = sock.accept()[0]
                except Exception:
                    continue
                #  LOGGER.info(f"Connection from {address[0]}")
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
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                try:
                    message_dict = json.loads(message_str)
                    if message_dict["message_type"] == "shutdown":
                        LOGGER.info("Manager shutdown signal")
                        shut_down(self.workers)
                        self.signals['shutdown'] = True
                    elif message_dict["message_type"] == 'register':
                        #  receive register message from worker
                        worker_host = message_dict['worker_host']
                        worker_port = message_dict['worker_port']
                        self.worker_registration(worker_host, worker_port)
                    #  check if finished update self.var
                    elif message_dict["message_type"] == 'new_manager_job':
                        #  new job request[manager]
                        self.new_job_request(message_dict)
                    #  check if worker finished a task
                    elif message_dict["message_type"] == "finished":
                        task_id = message_dict['task_id']
                        if self.is_mapping:
                            self.tasks[task_id].is_completed = True
                        else:
                            self.reduce_tasks[task_id].is_completed = True
                        worker_id = self.tasks[task_id].worker_id
                        self.tasks_completed += 1
                        self.workers[worker_id].status = "ready"
                except Exception:
                    continue
            time.sleep(1)

    def new_job_request(self, message_dict):
        """Pylint error handling comment."""
        job_dict = copy.deepcopy(message_dict)
        job_dict['job_id'] = self.job_id
        self.job_queue.put(job_dict)
        self.job_id += 1

    def job_to_task(self, job, tmpdir):
        """Pylint error handling comment."""
        #  self.job_tmp_dir = str(tmpdir)
        files = sorted(list(Path(job["input_directory"]).iterdir()))
        partitions = {}
        for index, filename in enumerate(files):
            task_id = index % job['num_mappers']
            if task_id in partitions:
                partitions[task_id].append(str(filename))
            else:
                partitions[task_id] = [str(filename)]
        map_tasks = {}
        for key, value in partitions.items():
            new_message = {
                            "message_type": "new_map_task",
                            "task_id": key,
                            "input_paths": value,
                            "executable": job['mapper_executable'],
                            "output_directory": str(tmpdir),
                            "num_partitions": job['num_reducers'],
                            "worker_host": -1,
                            "worker_port": -1
                            }
            task = Task(new_message)
            map_tasks[key] = task
        return map_tasks

    def run_the_jobs(self):
        """Pylint error handling comment."""
        LOGGER.info("run the jobs")
        while not self.signals['shutdown']:
            LOGGER.info("checking job queue")
            if not self.job_queue.empty():
                LOGGER.info("check if is working on job")
                if not self.is_working_on_job:
                    LOGGER.info("checking job queue")
                    self.is_working_on_job = True
                    self.curr_job = self.job_queue.get()
                    run_job_thread = threading.Thread(target=self.run_job)
                    LOGGER.info("starting run job thread")
                    run_job_thread.start()
                    #  LOGGER.info("joining run job thread")
                    run_job_thread.join()
            time.sleep(1)
        LOGGER.info("run the jobs has finished")

    def mapping(self):
        """Pylint error handling comment."""
        worker_index = 0
        #  First loop, assign tasks
        while not self.signals['shutdown'] and self.assigned_tasks < len(self.tasks):
            if self.signals['shutdown']:
                break
            if len(self.workers) == 0:
                continue
            current_worker = self.workers[worker_index % len(self.workers)]
            if current_worker.status == "ready":
                self.tasks[self.assigned_tasks].message["worker_host"] = current_worker.host
                self.tasks[self.assigned_tasks].message["worker_port"] = current_worker.port
                try:
                    current_worker.send_msg(self.tasks[self.assigned_tasks].message)
                    #  self.tasks[self.assigned_tasks].is_assigned = True
                    self.tasks[self.assigned_tasks].worker_id = current_worker.id
                    current_worker.task_id = self.assigned_tasks
                    current_worker.status = "busy"
                    self.assigned_tasks += 1
                except ConnectionRefusedError:
                    current_worker.status = 'dead'
            worker_index += 1
            time.sleep(1)
        #  Second loop, reassign other tasks
        while not self.signals['shutdown'] and self.tasks_completed < len(self.tasks):
            if self.signals['shutdown']:
                break
            for task_index, task in self.tasks.items():
                if self.signals['shutdown']:
                    break
                #  LOGGER.info(f"Task {task_index} reassign = {task.needs_reassign}")
                if task.needs_reassign:
                    while not self.signals['shutdown'] and task.needs_reassign:
                        if self.signals['shutdown']:
                            break
                        if len(self.workers) == 0:
                            continue
                        current_worker = self.workers[worker_index % len(self.workers)]
                        if current_worker.status == "ready":
                            task.message["worker_host"] = current_worker.host
                            task.message["worker_port"] = current_worker.port
                        try:
                            for worker in self.workers:
                                LOGGER.info(f"{worker.id} {worker.status} ")
                            current_worker.send_msg(task.message)
                            LOGGER.info(f"Sent reassign to worker {current_worker.id} ")
                            task.worker_id = current_worker.id
                            current_worker.task_id = task_index
                            current_worker.status = "busy"
                            task.needs_reassign = False
                        except ConnectionRefusedError:
                            current_worker.status = 'dead'
                            LOGGER.info(f"Error reassign")
                    worker_index += 1
            time.sleep(1)

    def run_job(self):
        """Pylint error handling comment."""
        LOGGER.info("Starting run_job")
        popped_job = self.curr_job
        popped_job_id = popped_job['job_id']
        #  Check if output directory exists
        output_directory = popped_job['output_directory']
        output_path = Path(output_directory)
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
        os.makedirs(output_path)
        prefix = f"mapreduce-shared-job{popped_job_id:05d}-"
        #  mapping_completed = False
        self.assigned_tasks = 0
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            #  tasks dictionary maps taskid to task object
            self.tasks = self.job_to_task(popped_job, tmpdir)
            LOGGER.info("Created tmpdir %s", tmpdir)
            #  mapping
            LOGGER.info("Calling mapping %s", tmpdir)
            self.is_mapping = True
            self.mapping()
            #  Reduce code
            self.tasks_completed = 0
            self.assigned_tasks = 0
            self.tasks.clear()
            self.is_mapping = False
            LOGGER.info("Calling reducing %s", tmpdir)
            self.reducing(tmpdir)
        self.is_working_on_job = False
        LOGGER.info("Cleaned up tmpdir %s", tmpdir)

    def worker_registration(self, worker_host, worker_port):
        """Pylint error handling comment."""
            #  Revive worker already present
        if worker_port in self.hp_id:
            worker_id = self.hp_id[worker_port]
            if self.workers[worker_id].status == 'dead':
                self.workers[worker_id].status = 'ready'
                init_time = time.time()
                self.workers[worker_id].last_time = init_time
            #  If worker isn't present, treat it normally
        else:
            worker_id = len(self.workers)
            init_time = time.time()
            new_worker = WorkerRep(worker_host,
                                    worker_port,
                                    worker_id,
                                    init_time)
            self.workers.append(new_worker)
            #  Send ack message for either case
        ack_message = {"message_type": "register_ack",
                        "worker_host": worker_host,
                        "worker_port": worker_port}
        self.hp_id[worker_port] = worker_id
        if not self.udp_running:
            self.threads.append(self.udp)
            self.udp.start()
            self.udp_running = True
        try:
            new_worker.send_msg(ack_message)
        except ConnectionRefusedError:
            new_worker.status = 'dead'

    def reducing(self, tmpdir):
        """Pylint error handling comment."""
        LOGGER.info("Starting reducing")
        executable = self.curr_job['reducer_executable']
        output_directory = self.curr_job['output_directory']
        num_partitions = self.curr_job["num_reducers"]
        #  Dictionary that maps task id to a Partition object
        for index in range(num_partitions):
            input_files = sorted(list(Path(tmpdir).glob(f"*-part{index:05d}")))
            sorted_files = [str(path) for path in input_files]
            new_task = ReduceTask(sorted_files)
            self.reduce_tasks[index] = new_task
        #  round-robin
        self.assigned_tasks = 0
        worker_index = 0

        while not self.signals['shutdown'] and self.assigned_tasks < num_partitions:
            if len(self.workers) == 0:
                continue
            current_worker = self.workers[worker_index % len(self.workers)]
            if current_worker.status == "ready":
                task = self.reduce_tasks[self.assigned_tasks]
                reduce_msg = {
                    "message_type": "new_reduce_task",
                    "task_id": self.assigned_tasks,
                    "executable": executable,
                    "input_paths": task.file_list,
                    "output_directory": output_directory,
                    "worker_host": current_worker.host,
                    "worker_port": current_worker.port
                }
                try:
                    current_worker.send_msg(reduce_msg)
                    task.worker_id = current_worker.id
                    current_worker.task_id = self.assigned_tasks
                    current_worker.status = "busy"
                    self.assigned_tasks += 1
                except ConnectionRefusedError:
                    current_worker.status = 'dead'
            worker_index += 1
            time.sleep(1)

        while not self.signals['shutdown'] and self.tasks_completed < num_partitions:
            for task_index, task in self.reduce_tasks.items():
                if task.needs_reassign:
                    while not self.signals['shutdown'] and task.needs_reassign:
                        if len(self.workers) == 0:
                            continue
                        current_worker = self.workers[worker_index % len(self.workers)]
                        if current_worker.status == "ready":
                            reduce_msg = {
                                "message_type": "new_reduce_task",
                                "task_id": self.assigned_tasks,
                                "executable": executable,
                                "input_paths": task.file_list,
                                "output_directory": output_directory,
                                "worker_host": current_worker.host,
                                "worker_port": current_worker.port
                            }
                            try:
                                current_worker.send_msg(reduce_msg)
                                task.worker_id = current_worker.id
                                current_worker.task_id = task_index
                                current_worker.status = "busy"
                                task.needs_reassign = False
                            except ConnectionRefusedError:
                                current_worker.status = 'dead'
                        worker_index += 1
            time.sleep(1)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="debug")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
