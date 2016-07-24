from common_utils.simple_logging import *
from common_utils.binary_tester import BinaryTester
from common_utils.poll_sanitizer import generate_poll_from_input
import os
from ..farnsworth_api_wrapper import CRSAPIWrapper


def _generate_poll(curr_poller_job):
    """
        Method to generate poll xml from Poller job.
    :param curr_poller_job: Poller job from which a valid poll needs to be generated.
    :return: Content of the new poll.
    """

    log_info("Trying to create Poll for Job:" + str(curr_poller_job.id))

    # get binary path
    bin_dir_path = os.path.join(os.path.expanduser("~"), 'pollcreator_' + str(curr_poller_job.id))
    os.system('mkdir -p ' + bin_dir_path)
    target_poll_content = None
    ret_code = -1
    try:
        # get original binary
        un_patched_bins = curr_poller_job.cs.cbns_original
        for curr_cb in un_patched_bins:
            curr_file = str(curr_cb.cs_id) + '_' + str(curr_cb.name)
            curr_file_path = os.path.join(bin_dir_path, curr_file)
            fp = open(curr_file_path, 'wb')
            fp.write(curr_cb.blob)
            fp.close()
            os.chmod(curr_file_path, 0o777)
        cs_name = curr_poller_job.cs.name

        # Get target test object
        target_test = curr_poller_job.target_test
        input_data = target_test.blob

        # generate poll from input
        target_poll_content, poll_test_res, ret_code = generate_poll_from_input(input_data, bin_dir_path,
                                                                                str(cs_name),
                                                                                optional_prefix=str(curr_poller_job.id) +
                                                                                                '_gen',
                                                                                log_suffix='For PollCreator Job:' +
                                                                                           str(curr_poller_job.id),
                                                                                afl_input=True)
        # set the flag so that, we will not try again.
        target_test.poll_created = True
        target_test.save()
        if target_poll_content is not None and poll_test_res == BinaryTester.PASS_RESULT:
            log_success("Successfully Generated PollXml for Job:" + str(curr_poller_job.id))
    except Exception as e:
        log_error("Error occurred:" + str(e) + " while trying to Generate Poll for Job:" + str(curr_poller_job.id))
    # clean up
    os.system('rm -rf ' + bin_dir_path)

    return target_poll_content, ret_code


def process_poll_creator_job(curr_job_args):
    """
    Process the provided job data, and update DB with corresponding result.
    :param curr_job_args:  (job that needs to run, Number of threads that could be used).
    :return: None
    """
    # CRSAPIWrapper.close_connection()
    CRSAPIWrapper.open_connection()
    curr_job = curr_job_args[0]
    target_job = curr_job

    if target_job.try_start():
        try:
            generated_poll_xml, ret_code = _generate_poll(curr_job)
            if generated_poll_xml is not None:
                # create a valid poll in the db.
                CRSAPIWrapper.create_valid_poll(curr_job.cs, generated_poll_xml, test=curr_job.target_test,
                                                is_perf_ready=(ret_code == 0))
        except Exception as e:
            log_error("Error Occurred while processing PollerJob:" + str(target_job.id) + ". Error:" + str(e))
        target_job.completed()
    else:
        log_failure("Ignoring PollerJob:" + str(target_job.id) + " as we failed to mark it busy.")
    CRSAPIWrapper.close_connection()
