from common_utils.simple_logging import log_info, log_success, log_failure, log_error
from farnsworth_api_wrapper import CRSAPIWrapper
from pov_tester import process_povtester_job
from poll_creator import process_poll_creator_job
from poll_sanitizer import process_sanitizer_job
from multiprocessing import Pool, cpu_count
import time
import sys

worker_config = [('povtester', CRSAPIWrapper.get_all_povtester_jobs, process_povtester_job),
                 ('poll_creator', CRSAPIWrapper.get_all_poller_jobs, process_poll_creator_job),
                 ('poll_sanitizer', CRSAPIWrapper.get_all_poll_sanitizer_jobs, process_sanitizer_job)]

NO_OF_PROCESSES = cpu_count()
POLL_TIME = 1  # Time to sleep, if no jobs are available to run


def run_daemon(arg_list):
    global NO_OF_PROCESSES
    global POLL_TIME

    if len(arg_list) > 1:
        try:
            no_of_process = int(arg_list[1])
        except ValueError:
            no_of_process = cpu_count()
        NO_OF_PROCESSES = no_of_process
    if len(arg_list) > 2:
        try:
            poll_time = int(arg_list[2])
        except ValueError:
            poll_time = POLL_TIME
        POLL_TIME = poll_time
    while True:
        CRSAPIWrapper.open_connection()
        no_jobs = True
        for curr_worker_config in worker_config:
            worker_name = curr_worker_config[0]
            job_getter = curr_worker_config[1]
            job_processor = curr_worker_config[2]
            log_info("Trying to get " + worker_name + " Jobs.")
            available_jobs = job_getter()
            if len(available_jobs) > 0:
                log_info("Got " + str(len(available_jobs)) + " " + worker_name + " Jobs.")
                child_threads = NO_OF_PROCESSES / len(available_jobs)
                child_job_args = map(lambda curr_job: (curr_job, child_threads), available_jobs)
                process_pool = Pool(processes=NO_OF_PROCESSES)
                process_pool.map(job_processor, child_job_args)
                process_pool.close()
                process_pool.join()
                log_success("Processed " + str(len(available_jobs)) + " " + worker_name + " Jobs.")
                no_jobs = False
                # start again
                break
            else:
                log_info("No " + worker_name + " Jobs available to run.")
        if no_jobs:
            time.sleep(poll_time)

if __name__ == "__main__":
    # Command line arguments: workers.py <NO_OF_PROCESSES> <POLL_TIME_IN_SECONDS>
    run_daemon(sys.argv)