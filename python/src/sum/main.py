import os
import multiprocessing
import signal
import logging
from multiprocessing.connection import wait as wait_for_process_exit

from sum import InputWorker, RingWorker
from common import middleware

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
_WORKER_JOIN_TIMEOUT_SECONDS = float(os.getenv("SUM_WORKER_JOIN_TIMEOUT_SECONDS", "5"))
_WORKER_KILL_TIMEOUT_SECONDS = float(os.getenv("SUM_WORKER_KILL_TIMEOUT_SECONDS", "2"))


class SumFilter:

    def __init__(self):
        self._p_input = None
        self._p_ring = None
        self._shutdown_requested = False

        tmp = None  # conexión temporal para declarar las colas antes de iniciar los workers.
        try:
            tmp = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
            for i in range(SUM_AMOUNT):
                tmp.declare_queue(f"{SUM_PREFIX}_ring_{i}")
        finally:
            if tmp:
                tmp.close()

        self._manager = multiprocessing.Manager()
        self._meta_by_client = self._manager.dict()
        self._partials_sum_by_client = self._manager.dict()
        self._lock = self._manager.Lock()

    def _workers(self):
        return (
            ("input_worker", self._p_input),
            ("ring_worker", self._p_ring),
        )

    def _start_workers(self):
        if self._shutdown_requested:
            return

        self._p_input = multiprocessing.Process(
            target=_run_input_worker,
            args=(self._meta_by_client, self._partials_sum_by_client, self._lock),
            daemon=True,
        )
        self._p_ring = multiprocessing.Process(
            target=_run_ring_worker,
            args=(self._meta_by_client, self._partials_sum_by_client, self._lock),
            daemon=True,
        )

        if self._shutdown_requested:
            return

        self._p_input.start()

        if self._shutdown_requested:
            self._p_input.terminate()
            self._p_input.join(timeout=_WORKER_JOIN_TIMEOUT_SECONDS)
            return

        self._p_ring.start()

    def _wait_until_a_worker_exits(self):
        if self._shutdown_requested:
            return

        sentinel_to_process = {}
        for worker_name, worker in self._workers():
            if worker is None:
                continue
            sentinel_to_process[worker.sentinel] = (worker_name, worker)

        if not sentinel_to_process:
            return

        ready_sentinels = wait_for_process_exit(list(sentinel_to_process.keys()))
        for sentinel in ready_sentinels:
            worker_name, worker = sentinel_to_process[sentinel]
            worker.join(timeout=0)
            logging.info(
                f"{worker_name} stopped | sum_id={ID} | exitcode={worker.exitcode}"
            )

    def _stop_workers_and_collect_failures(self):
        failures = []

        for worker_name, worker in self._workers():
            if worker is None or worker.pid is None:
                continue

            if worker.is_alive():
                logging.info(f"stopping {worker_name} | sum_id={ID}")
                worker.terminate()

            worker.join(timeout=_WORKER_JOIN_TIMEOUT_SECONDS)

            if worker.is_alive():
                logging.error(
                    f"{worker_name} did not stop after "
                    f"{_WORKER_JOIN_TIMEOUT_SECONDS}s | sum_id={ID}"
                )
                worker.kill()
                worker.join(timeout=_WORKER_KILL_TIMEOUT_SECONDS)

            if worker.is_alive():
                failures.append(f"{worker_name} stuck after SIGKILL ({worker.pid})")
                continue

            exitcode = worker.exitcode
            logging.info(f"{worker_name} exited | sum_id={ID} | exitcode={exitcode}")

            if self._shutdown_requested:
                if exitcode not in (0, -signal.SIGTERM):
                    failures.append(
                        f"{worker_name} exited unexpectedly during shutdown ({exitcode})"
                    )
            elif exitcode != 0:
                failures.append(f"{worker_name} failed with exit code {exitcode}")

        return failures

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        failures = []
        try:
            if self._shutdown_requested:
                return
            self._start_workers()
            self._wait_until_a_worker_exits()
        finally:
            try:
                failures.extend(self._stop_workers_and_collect_failures())
            finally:
                self._manager.shutdown()

        if failures:
            raise RuntimeError(
                "sum supervisor detected worker failures: " + "; ".join(failures)
            )

    def _handle_sigterm(self, signum, frame):
        logging.info(f"sigterm | component=sum | id={ID}")
        self._shutdown_requested = True

        for _, worker in self._workers():
            if not worker or not worker.is_alive():
                continue
            try:
                worker.terminate()
            except (ProcessLookupError, OSError, ValueError):
                pass


def _run_input_worker(meta_by_client, partials_by_client, lock):
    logging.basicConfig(level=logging.INFO)
    InputWorker(meta_by_client, partials_by_client, lock).run()


def _run_ring_worker(meta_by_client, partials_by_client, lock):
    logging.basicConfig(level=logging.INFO)
    RingWorker(meta_by_client, partials_by_client, lock).run()


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info(
        "starting | component=sum | "
        f"id={ID} | sum_amount={SUM_AMOUNT} | agg_amount={AGGREGATION_AMOUNT}"
    )

    try:
        sum_filter = SumFilter()
        sum_filter.start()
        return 0
    except Exception as e:
        logging.error(f"sum terminated with error | id={ID} | error={e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
