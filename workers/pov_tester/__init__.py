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


def _get_all_cbns(cs_fielded_obj):
    """
        Get all cbns of the provided fielded cs
    :param cs_fielded_obj: fielded cs for which we need to get the CBns for.
    :return: list of cbs of the provided fielded cs.
    """
    # TODO: fill this
    ret = []
    return ret


def _get_ids_rules_obj(ids_fielding_obj):
    """
        Get the ids rule object of the provided ids fielding obj.
    :param ids_fielding_obj: fielded IDS for which we need to get rules for.
    :return: ids_rules obj of the provided ids_fielding_obj
    """
    # TODO: fill this
    ids_rules_obj = None
    return ids_rules_obj


def _get_job_args(curr_pov_test_job):
    """
        Get arguments for a provided test job.
    :param curr_pov_test_job: pov test job for which arguments needs to be fetched.
    :return: (bin_dir, work_dir, pov_file_path) tuple
    """
    # TODO: Verify this
    cs_fielding_obj = curr_pov_test_job.target_cs_fielding
    pov_test_job_id = curr_pov_test_job.id
    # Get all binaries in
    all_cbns = _get_all_cbns(cs_fielding_obj)
    curr_work_dir = os.path.join(os.path.expanduser("~"), "pov_tester_" + str(pov_test_job_id))

    bin_dir = os.path.join(curr_work_dir, 'bin_dir')
    pov_dir = os.path.join(curr_work_dir, 'pov_dir')
    ids_dir = os.path.join(curr_work_dir, 'ids_rules')

    # set up binaries
    # Save CBNs into the bin dir
    os.system('mkdir -p ' + str(bin_dir))
    for curr_cb in all_cbns:
        curr_file = str(curr_cb.cs_id) + '_' + str(curr_cb.name)
        curr_file_path = os.path.join(bin_dir, curr_file)
        fp = open(curr_file_path, 'wb')
        fp.write(curr_cb.blob)
        fp.close()
        os.chmod(curr_file_path, 0o777)

    pov_file_path = None

    # set up povs
    # save povs into pov directory
    os.system('mkdir -p ' + str(pov_dir))
    target_exploit_obj = curr_pov_test_job.target_exploit
    pov_file_path = os.path.join(pov_dir, str(curr_pov_test_job.id) + '.pov')
    fp = open(pov_file_path, 'w')
    fp.write(target_exploit_obj.blob)
    fp.close()
    os.chmod(pov_file_path, 0o777)

    ids_file_path = None
    # set up ids rules
    # save ids rules into directory
    os.system('mkdir -p ' + str(ids_dir))
    ids_rules_obj = _get_ids_rules_obj(curr_pov_test_job.target_ids_fielding)
    if ids_rules_obj is not None and ids_rules_obj.rules is not None:
        ids_file_path = os.path.join(ids_dir, str(curr_pov_test_job.id) + '_ids.rules')
        fp = open(ids_file_path, 'w')
        fp.write(str(ids_rules_obj.rules))
        fp.close()
        os.chmod(ids_file_path, 0o777)

    return bin_dir, curr_work_dir, pov_file_path, ids_file_path


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
            CRSAPIWrapper.create_pov_test_result(curr_job.target_cs_fielding, curr_job.target_ids_fielding,
                                                 throws_passed)
            log_success("Done Processing PovTesterJob:" + job_id_str)
        except Exception as e:
            log_error("Error Occured while processing PovTesterJob:" + job_id_str + ". Error:" + str(e))
        target_job.completed()
    else:
        log_failure("Ignoring PovTesterJob:" + job_id_str + " as we failed to mark it busy.")
    CRSAPIWrapper.close_connection()
