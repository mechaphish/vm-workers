from .patch_tester import PatchTester, get_unique_dir
import os
from common_utils.simple_logging import log_failure, log_info, log_success
from ..farnsworth_api_wrapper import CRSAPIWrapper


def process_cb_tester_job(job_args):
    """
        Process the cb tester job
    :param job_args: Tuple (cb tester job, num process) to be tested.
    :return: None
    """
    CRSAPIWrapper.close_connection()
    CRSAPIWrapper.open_connection()
    curr_cb_test_job = job_args[0]
    no_process = job_args[1]
    curr_job_id = str(curr_cb_test_job.id)
    if curr_cb_test_job.try_start():
        log_info("Trying to process cb-tester Job:" + str(curr_job_id))
        target_dir = get_unique_dir(os.path.expanduser("~"), "cb_tester_" + str(curr_job_id))
        try:
            # save binaries and xml
            bin_dir = os.path.join(target_dir, "bin")
            xml_dir = os.path.join(target_dir, "poll_xml")
            ids_rule_fp = None
            ids_rule = None

            os.system('mkdir -p ' + str(bin_dir))
            os.system('mkdir -p ' + str(xml_dir))

            # save all the binaries in bin folder
            for curr_cb in CRSAPIWrapper.get_cbs_from_patch_type(curr_cb_test_job.target_cs,
                                                                 curr_cb_test_job.patch_type):
                bin_path = os.path.join(bin_dir, curr_cb.name)
                ids_rule = curr_cb.ids_rule
                fp = open(bin_path, 'wb')
                fp.write(curr_cb.blob)
                fp.close()
                os.chmod(bin_path, 0o777)

            # Save IDS rules
            if ids_rule is not None and ids_rule.rules is not None:
                ids_dir = os.path.join(target_dir, "ids_dir")
                os.system('mkdir -p ' + ids_dir)
                ids_rule_fp = os.path.join(ids_dir, "ids.rules")
                fp = open(ids_rule_fp, "w")
                fp.write(str(ids_rule.rules))
                fp.close()

            # save the xml
            xml_file_path = os.path.join(xml_dir, str(curr_cb_test_job.poll.id) + '.xml')
            fp = open(xml_file_path, 'w')
            fp.write(str(curr_cb_test_job.poll.blob))
            fp.close()

            # Test the poll
            curr_patch_tester = PatchTester(bin_dir, xml_file_path, ids_rule_fp, num_threads=no_process)
            curr_patch_tester.test()

            # get all perfs if poll is ok.
            perf_measurements = {}
            is_poll_ok = False
            if curr_patch_tester.are_polls_ok():
                is_poll_ok = True
                perf_measurements = curr_patch_tester.get_perf_measures()
            else:
                log_failure("CS:" + str(curr_cb_test_job.target_cs.id) + ", Patch Type:" +
                            str(curr_cb_test_job.patch_type) + " failed for Poll:" + str(curr_cb_test_job.poll.id))
            # update performance measurements.
            CRSAPIWrapper.create_poll_performance(curr_cb_test_job.poll, curr_cb_test_job.target_cs,
                                                  curr_cb_test_job.patch_type, is_poll_ok=is_poll_ok,
                                                  perf_json=perf_measurements)
            log_success("Processed cb-tester Job:" + str(curr_job_id))
            # mark job as completed.
        except Exception as e:
            log_failure("Exception occurred while trying to process cb_tester job:" + str(curr_job_id) +
                        ", Exception:" + str(e))
        curr_cb_test_job.completed()
        # clean up
        os.system('rm -rf ' + target_dir)
    else:
        log_info("Unable to start job:" + str(curr_job_id) + ". Ignoring")
    CRSAPIWrapper.close_connection()
