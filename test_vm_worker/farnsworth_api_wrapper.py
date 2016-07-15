import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
from farnsworth.models import NetworkPollSanitizerJob, CBTesterJob, PollCreatorJob, PovTesterJob, ChallengeSet, \
                              ValidPoll, CBPollPerformance, PovTestResult, TesterResult
import farnsworth.config


class CRSAPIWrapper:
    """
    Wrapper for farnsworth client
    """

    @staticmethod
    def open_connection():
        farnsworth.config.connect_dbs()

    @staticmethod
    def close_connection():
        farnsworth.config.close_dbs()

    @staticmethod
    def get_all_poll_sanitizer_jobs(target_cs_id=None):
        """
        Get all PollSanitizer Jobs Ready to run.
        :param target_cs_id: CS ID for which the Jobs needs to be fetched.
        :return: List of all PollSanitizer Jobs.
        """
        target_cs = None
        if target_cs_id is not None:
            target_cs = CRSAPIWrapper.get_cs_from_id(target_cs_id)
        if target_cs is None:
            all_poll_san_jobs = list(NetworkPollSanitizerJob.unstarted())
        else:
            all_poll_san_jobs = list(NetworkPollSanitizerJob.unstarted(cs=target_cs))

        return all_poll_san_jobs

    @staticmethod
    def get_all_cb_tester_jobs(target_cs_id=None):
        """
        Get all cb tester jobs Ready to run.
        :param target_cs_id: CS ID for which the Jobs needs to be fetched.
        :return: List of all tester jobs, ready to run.
        """
        target_cs = None
        if target_cs_id is not None:
            target_cs = CRSAPIWrapper.get_cs_from_id(target_cs_id)
        if target_cs is None:
            all_tests = list(CBTesterJob.unstarted())
        else:
            all_tests = list(CBTesterJob.unstarted(cs=target_cs))
        return all_tests

    @staticmethod
    def get_all_poller_jobs(target_cs_id=None):
        """
        Get all Poller Jobs Ready to run.
        :param target_cs_id: CS ID for which the Jobs needs to be fetched.
        :return: List of all Poller Jobs.
        """
        target_cs = None
        if target_cs_id is not None:
            target_cs = CRSAPIWrapper.get_cs_from_id(target_cs_id)
        if target_cs is None:
            all_poller_jobs = list(PollCreatorJob.unstarted())
        else:
            all_poller_jobs = list(PollCreatorJob.unstarted(cs=target_cs))
        return all_poller_jobs

    @staticmethod
    def get_all_povtester_jobs(target_cs_id=None):
        """
        Get all PovTesterJobs Ready to run.
        :param target_cs_id: CS ID for which the Jobs needs to be fetched.
        :return: List of all PovTester Jobs, that need to run
        """
        target_cs = None
        if target_cs_id is not None:
            target_cs = CRSAPIWrapper.get_cs_from_id(target_cs_id)
        if target_cs is None:
            all_povtester_jobs = list(PovTesterJob.unstarted())
        else:
            all_povtester_jobs = list(PovTesterJob.unstarted(cs=target_cs))
        return all_povtester_jobs

    @staticmethod
    def get_binary_path(test_job):
        """
        Get path of the binary corresponding to this test job
        :param test_job: Test job for which binary path needs to be fetched.
        :return: File path of the binary
        """
        filename = "{}-{}-{}".format(test_job.id, test_job.cbn.cs_id, test_job.cbn.name)
        target_path = os.path.join(os.path.expanduser("~"), filename)
        if not os.path.isfile(target_path):
            open(target_path, 'wb').write(test_job.cbn.blob)
            os.chmod(target_path, 0o777)
        return target_path

    @staticmethod
    def get_testcase_path(test_job):
        """
            Get XML of the test corresponding to current test job
        :return: Absolute path
        """
        # TODO: Check here that we get xml of the test case
        filepath = os.path.join(os.path.expanduser("~"),
                                str(test_job.id) + '_' + str(test_job.target_test.id) + '_test.xml')
        if not os.path.isfile(filepath):
            open(filepath, 'w').write(test_job.target_test.to_cqe_pov_xml())
            os.chmod(filepath, 0o777)
        return filepath

    @staticmethod
    def get_cbs_from_patch_type(target_cs, target_patch_type):
        """
            Get all binaries the belong to the provided patched id and cs.

        :param target_cs: CS for which the binaries need to be fetched.
        :param target_patch_type: Patch type of the CS for which cbns need to be fetched.
        :return: list of binaries of the provided patch
        """
        # This means original cbns
        if target_patch_type is None:
            return CRSAPIWrapper.get_unpatched_cbs(target_cs)
        patch_type_cbns = target_cs.cbns_by_patch_type()
        if target_patch_type in patch_type_cbns:
            return patch_type_cbns[target_patch_type]
        return []

    @staticmethod
    def get_unpatched_cbs(target_cs):
        """
            Get unpatched CBs for a given CS.
        :param target_cs: ChallengeSet for which unpatched binaries need to be fetched.
        :return: List of unpatched CBNS of the given CS.
        """
        return target_cs.cbns_unpatched

    @staticmethod
    def get_cs_from_id(target_cs_id):
        """
            Get ChallengeSet for given id
        :param target_cs_id: id for which ChallengeSet need to be fetched.
        :return:  ChallengeSet
        """
        return ChallengeSet.get(ChallengeSet.id == target_cs_id)

    @staticmethod
    def update_testjob_completed(test_job, error_code, result, stdout_out, stderr_out, performance_json):
        """
        Update the provided job as completed.
        :param test_job: The target job whose status needs to be updated.
        :param error_code:
        :param result
        :param stdout_out
        :param stderr_out
        :param output: Produced output of this job.
        :param performance_json: Json containing the performance counter of
                the given job.
        :return: True if successful else false
        """
        # Create tester result.
        TesterResult.create(job=test_job, error_code=int(error_code), result=result, stdout_out=stdout_out,
                            stderr_out=stderr_out, performances=performance_json)
        # mark the corresponding test job as complete
        test_job.completed()
        return test_job.is_completed()

    @staticmethod
    def create_valid_poll(target_cs, poll_xml_content, test=None, target_round=None, is_perf_ready=True):
        """
        Create a valid poll corresponding to this poller job.
        :param target_cs: Poller job for which poll needs to be created.
        :param poll_xml_content: xml contents of the generated poll
        :param test: Target Test for which poll has been created.
        :param target_round: Target round during which poll has been crated.
        :param is_perf_ready: Flag to indicate that this poll could be used to measure performance.
        :return: None
        """
        ValidPoll.create(cs=target_cs, test=test, is_perf_ready=is_perf_ready, round=target_round,
                         blob=poll_xml_content)

    @staticmethod
    def create_poll_performance(target_poll, target_cs, patch_type, is_poll_ok=True, perf_json=None):
        """
        Create a poll performance object.
        :param target_poll: cbtest poll for which the results should be updated.
        :param target_cs: CS for which performance need to be updated.
        :param patch_type: Patch type for which performance needs to be updated.
        :param is_poll_ok: flag to indicate whether the poll is successful.
        :param perf_json: performance json.
        :return: None
        """
        CBPollPerformance.create(poll=target_poll, cs=target_cs, patch_type=patch_type, is_poll_ok=is_poll_ok,
                                 performances=perf_json)

    @staticmethod
    def create_pov_test_result(cs_fielding, ids_fielding, num_success, test_feedback=None):
        """
        Create Pov Test Result.
        :param cs_fielding: CS Fielding for which the result should be updated.
        :param ids_fielding: IDS Fielding for which the result should be updated.
        :param num_success: number of successful throws.
        :param test_feedback: Feedback from testing, if any.
        :return:
        """
        # TODO: Check this?
        PovTestResult.create(cs_fielding=cs_fielding, ids_fielding=ids_fielding, num_success=num_success,
                             test_feedback=test_feedback)

