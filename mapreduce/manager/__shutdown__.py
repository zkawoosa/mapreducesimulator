"""Pylint error handling comment."""
import socket
#  import pathlib
#  import threading
#  import time
import json
import logging

LOGGER = logging.getLogger(__name__)


def shut_down(registered_workers):
    """Pylint error handling comment."""
    #  forward message to workers
    for worker in registered_workers:
        if worker.status == "dead":
            continue
        #  LOGGER.info(f"sending shutdown to %s", worker)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            #  connect to the server
            sock.connect((worker.host, worker.port))
            #  send a message
            message = json.dumps({"message_type": "shutdown"})
            sock.sendall(message.encode('utf-8'))
            worker.status = "dead"
