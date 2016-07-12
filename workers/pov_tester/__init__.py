from ..farnsworth_api_wrapper import CRSAPIWrapper
from common_utils.binary_tester import BinaryTester
from multiprocessing import Pool, cpu_count
from common_utils.simple_logging import log_info, log_success, log_failure, log_error
import os
import sys

NUM_THROWS = 12


def _test_pov(thread_arg):
    """
        Test the provided PoV.
    :param thread_arg: (bin_folder, pov_file, ids_rules) tuple.
    :return: True if successful else False.
    """
    bin_folder = thread_arg[0]
    pov_file = thread_arg[1]
    ids_rules = thread_arg[2]
    bin_tester = BinaryTester(bin_folder, pov_file, is_pov=True, is_cfe=True, standlone=True, ids_rules=ids_rules)
    ret_code, _, _ = bin_tester.test_cb_binary()
    return ret_code == 0


def _get_all_cbns(cb_fielded_set):
    """
        Get all cbns of the provided fielded cs
    :param cb_fielded_set: fielded cs for which we need to get the CBns for.
    :return: list of cbs of the provided fielded cs.
    """
    # TODO: fill this
    ret = []
    return ret


def _get_job_args(curr_pov_test_job):
    """
        Get arguments for a provided test job.
    :param curr_pov_test_job: pov test job for which arguments needs to be fetched.
    :return: (bin_dir, work_dir, pov_file_path) tuple
    """
    # TODO: Verify this
    cb_test_obj = curr_pov_test_job.fielded_set
    pov_test_job_id = curr_pov_test_job.id
    # Get all binaries in
    all_cbns = _get_all_cbns(cb_test_obj)
    curr_work_dir = os.path.join(os.path.expanduser("~"), "pov_tester_" + str(pov_test_job_id))

    bin_dir = os.path.join(curr_work_dir, 'bin_dir')
    pov_dir = os.path.join(curr_work_dir, 'pov_dir')

    # TODO: fill this up
    pov_file_path = None

    # set up binaries
    os.system('mkdir -p ' + str(bin_dir))
    # TODO: Save CBNs into the bin dir

    # set up povs
    os.system('mkdir -p ' + str(bin_dir))
    # TODO: Save povs into the pov dir

    return bin_dir, curr_work_dir, pov_file_path


def process_povtester_job(curr_job_args):
    """
        Process the provided PoV Tester Job with given number of threads.
    :param curr_job_args: (pov tester job to process, number of threads that could be used)
    :return: None
    """
    CRSAPIWrapper.close_connection()
    CRSAPIWrapper.open_connection()
    curr_job = curr_job_args[0]
    num_threads = curr_job_args[1]
    target_job = curr_job
    job_id_str = str(curr_job.id)

    if target_job.try_start():
        try:
            job_id_str = str(curr_job.id)
            log_info("Trying to run PovTesterJob:" + job_id_str)
            all_child_process_args = []
            # TODO: Fix this, get ids rules
            job_bin_dir, curr_work_dir, pov_file_path, ids_rules_path = _get_job_args(curr_job)
            for i in range(NUM_THROWS):
                all_child_process_args.append((job_bin_dir, pov_file_path, ids_rules_path))

            log_info("Got:" + str(len(all_child_process_args)) + " Throws to test for PovTesterJob:" + job_id_str)

            all_results = []
            # If we can multiprocess? Run in multi-threaded mode
            if num_threads > 1:
                log_info("Running in multi-threaded mode with:" + str(num_threads) + " threads. For PovTesterJob:" +
                         job_id_str)
                process_pool = Pool(processes=num_threads)
                all_results = process_pool.map(_test_pov, all_child_process_args)
            else:
                log_info("Running in single threaded mode. For PovTesterJob:" +
                         job_id_str)
                for curr_child_arg in all_child_process_args:
                    all_results.append(_test_pov(curr_child_arg))
            # clean up
            os.system('rm -rf ' + curr_work_dir)
            throws_passed = len(filter(lambda x: x, all_results))
            # TODO: Update the results into DB
            log_success("Done Processing PovTesterJob:" + job_id_str)
        except Exception as e:
            log_error("Error Occured while processing PovTesterJob:" + job_id_str + ". Error:" + str(e))
        target_job.completed()
    else:
        log_failure("Ignoring PovTesterJob:" + job_id_str + " as we failed to mark it busy.")
    CRSAPIWrapper.close_connection()


def run_daemon(arg_list):
    no_of_process = cpu_count()
    log_info("Trying to get Pov Test Jobs.")
    # TODO: get all jobs
    to_test_jobs = []
    log_success("Got:" + str(len(to_test_jobs)) + " number of Jobs.")
    all_workers = {}
    all_child_process_args = []

    log_info("Trying to compute jobs to schedule.")
    for curr_job in to_test_jobs:
        # TODO: check this
        worker_key = str(curr_job.id) + '_' + str(curr_job.fielded_set.id)
        job_bin_dir, curr_work_dir, pov_file_path = _get_job_args(curr_job)
        # create a state for all worker
        all_workers[worker_key] = [curr_job, job_bin_dir, curr_work_dir, pov_file_path, 0]
        # for each throw create a thread.
        for i in range(NUM_THROWS):
            all_child_process_args.append((job_bin_dir, pov_file_path, worker_key))

    log_success("Got:" + str(len(all_child_process_args)) + " jobs to run.")

    log_info("Scheduling " + str(len(all_child_process_args)) + " Jobs.")
    process_pool = Pool(processes=no_of_process)
    all_results = process_pool.map(_test_pov, all_child_process_args)
    log_success("Jobs Complete.")
    process_pool.close()
    # wait for the jobs to finish
    process_pool.join()

    log_info("Aggregating Results from Multiple Processes.")

    for curr_result in all_results:
        all_workers[curr_result[0]][4] += 1 if curr_result[1] else 0

    log_success("Aggregated Results.")

    log_info("Trying to update results into DB.")

    # update results
    for worker_args in all_workers.values():
        pov_test_job = worker_args[0]
        pov_work_dir = worker_args[2]
        num_successful_polls = worker_args[4]

        # clean up
        os.system('rm -rf ' + pov_work_dir)
        log_info("Trying to update results of Pov Test Job ID:" + str(pov_test_job.id))
        # update the results of POV testing in DB
        # TODO: fill up content here.
        log_success("Updated results of Pov Test Job ID:" + str(pov_test_job.id) + ", successful throws = " +
                    str(num_successful_polls))
