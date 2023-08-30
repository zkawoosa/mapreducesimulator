"""See unit test function docstring."""

import json
import logging
import threading
import subprocess
import mapreduce
import utils
from utils import TESTDATA_DIR, MemoryProfiler

# Set up logging
LOGGER = logging.getLogger("autograder")


def manager_message_generator(mock_sendall, memory_profiler, tmp_path):
    """Fake Manager messages."""
    # Worker register
    #
    # Transfer control back to solution under test in between each check for
    # the register message to simulate the Worker calling recv() when there's
    # nothing to receive.
    for _ in utils.wait_for_register_messages(mock_sendall):
        yield None

    yield json.dumps({
        "message_type": "register_ack",
        "worker_host": "localhost",
        "worker_port": 6001,
    }).encode("utf-8")
    yield None

    # Start tracking memory usage
    memory_profiler.start()

    # Map task
    yield json.dumps({
        "message_type": "new_map_task",
        "task_id": 0,
        "executable": TESTDATA_DIR/"exec/wc_map.sh",
        "input_paths": [
            TESTDATA_DIR/"input_large/file01",
            TESTDATA_DIR/"input_large/file02",
            TESTDATA_DIR/"input_large/file03",
            TESTDATA_DIR/"input_large/file04",
        ],
        "output_directory": tmp_path,
        "num_partitions": 1,
        "worker_host": "localhost",
        "worker_port": 6001,
    }, cls=utils.PathJSONEncoder).encode("utf-8")
    yield None

    # Wait for Worker to finish map task
    #
    # Transfer control back to solution under test in between each check for
    # the finished message to simulate the Worker calling recv() when there's
    # nothing to receive.
    for _ in utils.wait_for_status_finished_messages(mock_sendall):
        yield None

    memory_profiler.stop()

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode("utf-8")
    yield None


def test_map_memory(mocker, tmp_path):
    """Evaluate Worker's memory usage during Map Stage.

    Note: 'mocker' is a fixture function provided by the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.

    See https://github.com/pytest-dev/pytest-mock/ for more info.

    Note: 'tmp_path' is a fixture provided by the pytest-mock package.
    This fixture creates a temporary directory for use within this test.

    See https://docs.pytest.org/en/6.2.x/tmpdir.html for more info.
    """
    # Mock the socket library socket class
    mock_socket = mocker.patch("socket.socket")

    # sendall() records messages
    mock_sendall = mock_socket.return_value.__enter__.return_value.sendall

    # accept() returns a mock client socket
    mock_clientsocket = mocker.MagicMock()
    mock_accept = mock_socket.return_value.__enter__.return_value.accept
    mock_accept.return_value = (mock_clientsocket, ("127.0.0.1", 10000))

    # Initialize memory profiler, which tracks max memory usage
    memory_profiler = MemoryProfiler()

    # Count the number of calls to certain functions
    count_popen_calls = mocker.spy(subprocess, "Popen")
    forbidden_funcs = utils.get_forbidden_funcs(mocker)

    # recv() returns values generated by manager_message_generator()
    mock_recv = mock_clientsocket.recv
    mock_recv.side_effect = manager_message_generator(
        mock_sendall,
        memory_profiler,
        tmp_path,
    )

    # Run student Worker code.  When student Worker calls recv(), it will
    # return the faked responses configured above.  When the student code calls
    # sys.exit(0), it triggers a SystemExit exception, which we'll catch.
    try:
        mapreduce.worker.Worker(
            host="localhost",
            port=6001,
            manager_host="localhost",
            manager_port=6000,
        )
        assert threading.active_count() == 1, "Failed to shutdown threads"
    except SystemExit as error:
        assert error.code == 0

    # Verify messages sent by the Worker
    #
    # Pro-tip: show log messages and detailed diffs with
    #   $ pytest -vvs --log-cli-level=info tests/test_worker_X.py
    all_messages = utils.get_messages(mock_sendall)
    messages = utils.filter_not_heartbeat_messages(all_messages)
    assert messages[:2] == [
        {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        },
        {
            "message_type": "finished",
            "task_id": 0,
            "worker_host": "localhost",
            "worker_port": 6001,
        },
    ]

    # We expect one subprocess.Popen() call per input file
    assert count_popen_calls.call_count == 4

    # Certain process-creating functions should never be called
    utils.check_forbidden_funcs(forbidden_funcs)

    # Verify time and memory constraints
    map_time_seconds = memory_profiler.get_time_delta()
    map_memory_bytes = memory_profiler.get_mem_delta()
    LOGGER.info("Map time: %f s", map_time_seconds)
    LOGGER.info("Map memory: %f MB", map_memory_bytes / 1024 / 1024)
    assert map_memory_bytes < 55 * 1024 * 1024  # 55 MB
    assert 0 < map_time_seconds < 10
