from ..farnsworth_api_wrapper import CRSAPIWrapper
from farnsworth.actions import cfe_poll_from_xml, Write
from common_utils.simple_logging import log_success, log_failure, log_error, log_info
from common_utils.poll_sanitizer import sanitize_pcap_poll
from common_utils.binary_tester import BinaryTester
import os


def get_write_data_from_poll(xml_blob):
    """
        Get Data from all write tags from poll xml
    :param xml_blob: Xml blob from which xml need to be fetched.
    :return: Write data as string.
    """
    cfe_poll_xml = cfe_poll_from_xml(xml_blob)
    all_writes = filter(lambda curr_action: isinstance(curr_action, Write), cfe_poll_xml.actions)
    write_data = None
    if len(all_writes) > 0:
        for i in range(len(all_writes)):
            curr_write = all_writes[i]
            for curr_data in curr_write.data_vars:
                write_data += str(curr_data.data)
    return write_data


def process_sanitizer_job(curr_job_args):
    """
        Process the provided sanitizer job.
        and update DB with corresponding valid poll, else update the
        raw poll with reason of failure.
    :param curr_job_args:  (job that needs to run, num threads)
                            (As of now, we ignore num threads as it is not needed)
    :return: None
    """
    # CRSAPIWrapper.close_connection()
    CRSAPIWrapper.open_connection()
    curr_job = curr_job_args[0]
    target_job = curr_job

    if target_job.try_start():
        target_cbs_path = os.path.join(os.path.expanduser('~'), 'pollsan_' + str(curr_job.id))
        try:
            log_info("Trying to process PollSanitizerJob:" + str(target_job.id))
            # Create folder to save all binaries that belong to current CS.
            target_raw_poll = curr_job.raw_poll

            os.system('mkdir -p ' + str(target_cbs_path))
            # Save all binaries
            for curr_cb in target_raw_poll.cs.cbns_original:
                curr_file = str(curr_cb.cs_id) + '_' + str(curr_cb.name)
                curr_file_path = os.path.join(target_cbs_path, curr_file)
                fp = open(curr_file_path, 'wb')
                fp.write(curr_cb.blob)
                fp.close()
                os.chmod(curr_file_path, 0o777)

            sanitized_xml, target_result, ret_code = sanitize_pcap_poll(target_raw_poll.blob,
                                                                        target_cbs_path,
                                                                        optional_prefix='pollsan_' + str(curr_job.id),
                                                                        log_suffix=' for PollSanitizerJob:' +
                                                                                   str(curr_job.id))
            target_raw_poll.sanitized = True
            target_raw_poll.save()

            if target_result == BinaryTester.CRASH_RESULT:
                # set crash to true
                target_raw_poll.is_crash = True
                log_error("PollSanitizerJob:" + str(curr_job.id) + ", Lead to Crash. Someone attacked us, "
                                                                   "it will be synced in network poll creator")
                target_raw_poll.save()
            elif target_result == BinaryTester.FAIL_RESULT:
                # set failed to true
                target_raw_poll.is_failed = True
                target_raw_poll.save()
                log_error("PollSanitizerJob:" + str(curr_job.id) + ", Failed on binary. Mostly timeout or Crash.")
            elif target_result == BinaryTester.PASS_RESULT:
                # Create Valid Poll
                CRSAPIWrapper.create_valid_poll(curr_job.raw_poll.cs, sanitized_xml,
                                                target_round=curr_job.raw_poll.round, is_perf_ready=(ret_code == 0))
                log_success("Created a ValidPoll for PollSanitizerJob:" + str(curr_job.id))
            else:
                log_error("Error occurred while sanitizing provided poll of Job:" + str(curr_job.id) +
                          ", Sanitize PCAP POLL Returned:" + str(target_result))

        except Exception as e:
            log_error("Error Occured while processing PollerSanitizerJob:" + str(target_job.id) + ". Error:" + str(e))
        # clean up
        os.system('rm -rf ' + str(target_cbs_path))
        target_job.completed()
    else:
        log_failure("Ignoring PollerSanitizerJob:" + str(target_job.id) + " as we failed to mark it busy.")
    CRSAPIWrapper.close_connection()
