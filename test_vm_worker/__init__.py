from common_utils.simple_logging import log_info, log_success, log_failure, log_error
from farnsworth_api_wrapper import CRSAPIWrapper
from pov_tester import process_povtester_job
from poll_creator import process_poll_creator_job
from poll_sanitizer import process_sanitizer_job
from cb_tester import process_cb_tester_job
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor
import time
import sys

worker_config = [('pov_tester', CRSAPIWrapper.get_all_povtester_jobs, process_povtester_job),
                 ('cb_tester', CRSAPIWrapper.get_all_cb_tester_jobs, process_cb_tester_job),
                 ('poll_creator', CRSAPIWrapper.get_all_poller_jobs, process_poll_creator_job),
                 ('network_poll_sanitizer', CRSAPIWrapper.get_all_poll_sanitizer_jobs, process_sanitizer_job)]

NO_OF_PROCESSES = cpu_count()
POLL_TIME = 1  # Time to sleep, if no jobs are available to run
# only for testing.
# Change this to false for testing.
EXIT_ON_WRONG_CS_ID = True


def run_daemon(arg_list):
    global NO_OF_PROCESSES
    global POLL_TIME
    target_cs_id = None
    target_job_type = None
    max_num_jobs = 1000

    if len(arg_list) > 1:
        try:
            target_cs_id = int(arg_list[1])
            log_info("Handling Jobs for CS ID:" + str(target_cs_id))
        except ValueError:
            log_failure("Unable to parse the provided argument into CS ID:" + str(arg_list[1]))
            # Ignore the error
            pass
    if len(arg_list) > 2:
        target_job_type = arg_list[2]
        log_info("Provided Job Type:" + str(target_job_type))
    else:
        log_failure("No job type provided")

    if len(arg_list) > 3:
        try:
            max_num_jobs = int(arg_list[3])
        except ValueError:
            log_failure("Unable to parse the provided argument into number of jobs:" + str(arg_list[3]))
            # default, number of jobs
            max_num_jobs = 1000
            pass
    else:
        log_failure("No max number of jobs provided, defaulting to:" + str(max_num_jobs))

    # Ignore this
    """if len(arg_list) > 2:
        try:
            no_of_process = int(arg_list[2])
        except ValueError:
            no_of_process = cpu_count()
        NO_OF_PROCESSES = no_of_process
    if len(arg_list) > 3:
        try:
            poll_time = int(arg_list[3])
        except ValueError:
            poll_time = POLL_TIME
        POLL_TIME = poll_time"""

    if EXIT_ON_WRONG_CS_ID and target_cs_id is None:
        log_error("Exiting, without scheduling any jobs as no valid CS ID is provided.")
        return
    elif target_cs_id is None:
        log_info("Will be running infinitely fetching Jobs for all CS.")
    processed_jobs = 0
    while True:
        CRSAPIWrapper.open_connection()
        no_jobs = True
        for curr_worker_config in worker_config:
            worker_name = curr_worker_config[0]
            if target_job_type is None or worker_name == target_job_type:
                job_getter = curr_worker_config[1]
                job_processor = curr_worker_config[2]
                log_info("Trying to get " + worker_name + " Jobs.")
                available_jobs = job_getter(target_cs_id=target_cs_id)
                if len(available_jobs) > 0:
                    num_jobs_to_get = max_num_jobs - processed_jobs
                    if num_jobs_to_get <= 0:
                        break
                    available_jobs = available_jobs[0:num_jobs_to_get]
                    processed_jobs += len(available_jobs)
                    log_info("Got " + str(len(available_jobs)) + " " + worker_name + " Jobs.")
                    if str(worker_name) == 'pov_tester':
                        NO_OF_PROCESSES = int(1.25 * NO_OF_PROCESSES)
                    child_threads = NO_OF_PROCESSES / len(available_jobs)
                    child_job_args = map(lambda curr_job: (curr_job.id, child_threads), available_jobs)
                    with ProcessPoolExecutor(max_workers=NO_OF_PROCESSES) as process_pool:
                        process_pool.map(job_processor, child_job_args)
                    log_success("Processed " + str(len(available_jobs)) + " " + worker_name + " Jobs.")
                    no_jobs = False
                    # start again from beginning.
                    # This will ensure the if there are any higher priority jobs, we always execute them
                    # before going to lower priority ones
                    break
                else:
                    log_info("No " + worker_name + " Jobs available to run.")

        # if there are no VM jobs
        if no_jobs:
            # If this is supposed to take care of only one CS.
            # exit the while True loop.
            # so that the worker knows this VM is done.
            if target_cs_id is not None:
                break
            time.sleep(POLL_TIME)
        # if we processed sufficient number of jobs? then exit
        if processed_jobs >= max_num_jobs:
            log_info("Processed:" + str(processed_jobs) + ", limit:" + str(max_num_jobs) + ". Exiting.")
            break

if __name__ == "__main__":
    # Command line arguments: common_tester <target_cs_id> <job_type_string> <max_num_jobs>
    run_daemon(sys.argv)
