import os
from multiprocessing import Pool, cpu_count
from common_utils.simple_logging import log_failure, log_info, log_success
from common_utils.binary_tester import BinaryTester


def get_unique_dir(base_dir, choice_dir):
    """

    :param base_dir:
    :param choice_dir
    :return:
    """
    target_dir = os.path.join(base_dir, choice_dir)
    curr_count = 1
    while os.path.exists(target_dir):
        target_dir = os.path.join(base_dir, choice_dir + '_' + str(curr_count))
        curr_count += 1
    os.system('mkdir -p ' + target_dir)
    return target_dir


def bin_tester(bin_dir, poll_xml):
    """
        Test the binaries in directory with a poll xml
    :param bin_dir: directory containing binaries.
    :param poll_xml: path of poll xml
    :return: (poll_xml, ret_code, has_perf, final_result, perf_json)
    """
    tester_obj = BinaryTester(bin_dir, poll_xml, standalone=True)
    ret_code, output_text, _ = tester_obj.test_cb_binary()
    has_perf, final_result, perf_json = BinaryTester.parse_cb_test_out(output_text)
    return poll_xml, ret_code, has_perf, final_result, perf_json


def bin_tester_wrapper(target_args):
    """
        Wrapper for bintester.
    :param target_args: args for bin tester.
    :return: return of bin tester.
    """
    return bin_tester(*target_args)


class PatchTester(object):

    RSS_PERF_NAME = "rss"
    FLT_PERF_NAME = "flt"
    UTIME_PERF_NAME = "utime"
    CPU_CLOCK_PERF_NAME = "cpu_clock"
    TSK_CLOCK_PERF_NAME = "task_clock"
    FILE_SIZE_PERF_NAME = "file_size"
    # To get nice median :)
    NUM_TEST_TIME = 11

    def __init__(self, bin_directory, poll_xml_path, num_threads=1):
        """
            Create a patch tester object.
        :param bin_directory: directory containing binaries to be tested.
        :param poll_xml_path: Xml path of the poll to test.
        :param num_threads: number of threads.
        :return: None
        """
        self.bin_directory = bin_directory
        self.poll_xml_path = poll_xml_path
        self.num_threads = num_threads
        # Sanity
        if self.num_threads > cpu_count():
            self.num_threads = cpu_count() - 1
        self.test_results = None

    def test(self):
        """
            Test the provided patch.
        :return: None
        """
        if self.test_results is None:
            self.test_results = []
            if os.path.exists(self.poll_xml_path):
                # if multi-threaded?
                if self.num_threads > 1:
                    log_info("Trying to test:" + self.bin_directory + " with poll xml:" + self.poll_xml_path +
                             " with " + str(self.num_threads) + " threads for " + str(PatchTester.NUM_TEST_TIME) +
                             " times")
                    # prepare all args
                    thread_args = []
                    for i in range(PatchTester.NUM_TEST_TIME):
                        thread_args.append((self.bin_directory, self.poll_xml_path))
                    # map to process poll
                    process_pool = Pool(processes=self.num_threads)
                    self.test_results = process_pool.map(bin_tester_wrapper, thread_args)
                    log_success("Tested:" + self.bin_directory + " with poll xml:" + self.poll_xml_path +
                                " with " + str(self.num_threads) + " threads for " + str(PatchTester.NUM_TEST_TIME) +
                                " times")
                    process_pool.terminate()
                    process_pool.join()

                else:
                    log_info("Trying to test:" + self.bin_directory + " with poll xml:" + self.poll_xml_path +
                             " in single threaded mode for " + str(PatchTester.NUM_TEST_TIME) +
                             " times")
                    # single threaded
                    # test each poll file individually.
                    for i in range(PatchTester.NUM_TEST_TIME):
                        self.test_results.append(bin_tester_wrapper(self.bin_directory, self.poll_xml_path))

                    log_success("Tested:" + self.bin_directory + " with poll xml dir:" + self.poll_xml_path +
                                " in single threaded mode for " + str(PatchTester.NUM_TEST_TIME) +
                                " times")

    def are_polls_ok(self):
        """
            Check if all runs of the poll are fine.
        :return: True/False depending on whether the poll is fine or not.
        """
        # just make sure that tests are already run
        self.test()
        # polls which did not succeed.
        failed_polls = filter(lambda curr_res: curr_res[3] != BinaryTester.PASS_RESULT, self.test_results)
        # this should be zero
        return len(failed_polls) == 0

    @staticmethod
    def __get_median(num_list):
        """
            Get median of list of numbers.
        :param num_list: list of numbers whose median needs to be computed.
        :return: median
        """
        to_ret_median = None
        sorted_list = sorted(num_list)
        if len(sorted_list) > 0:
            mid_val = len(sorted_list)/2
            if len(sorted_list) % 2 == 0:
                # if even, sum the middle 2
                to_ret_median = float(sorted_list[mid_val] + sorted_list[mid_val - 1]) / 2
            else:
                # if odd, return the middle element.
                to_ret_median = float(sorted_list[mid_val])
        return to_ret_median

    @staticmethod
    def __get_variance(num_list):
        """
            Get variance of the list of numbers.
        :param num_list: list of numbers for which variance needs to be computed.
        :return: Variance as float.
        """
        to_ret_variance = None
        if len(num_list) > 0:
            # compute mean
            list_mean = float(sum(num_list)) / len(num_list)
            # get variance nums for each value
            var_nums = map(lambda curr_val: (curr_val - list_mean)*(curr_val - list_mean), num_list)
            # compute average
            to_ret_variance = float(sum(var_nums)) / len(num_list)
        return to_ret_variance

    def get_perf_measures(self):
        """
            Get performance json of the poll run of the provided cb.
        :return: Json with median and variance of the perf numbers.
        """
        # Test, just in case
        self.test()
        log_info("Trying to compute performance measures.")
        # initialize to defaults
        perfs_median = {PatchTester.RSS_PERF_NAME: 0.0, PatchTester.FLT_PERF_NAME: 0.0,
                        PatchTester.CPU_CLOCK_PERF_NAME: 0.0, PatchTester.TSK_CLOCK_PERF_NAME: 0.0,
                        PatchTester.FILE_SIZE_PERF_NAME: 0.0}
        perfs_variance = {PatchTester.RSS_PERF_NAME: 0.0, PatchTester.FLT_PERF_NAME: 0.0,
                          PatchTester.CPU_CLOCK_PERF_NAME: 0.0, PatchTester.TSK_CLOCK_PERF_NAME: 0.0,
                          PatchTester.FILE_SIZE_PERF_NAME: 0.0}
        perf_keys = [PatchTester.RSS_PERF_NAME, PatchTester.FLT_PERF_NAME, PatchTester.CPU_CLOCK_PERF_NAME,
                     PatchTester.TSK_CLOCK_PERF_NAME, PatchTester.FILE_SIZE_PERF_NAME]
        all_ok = True
        # get all perf dicts
        all_perfs = map(lambda curr_res: curr_res[4]["perf"], self.test_results)
        # iterate thru each and compute the corresponding measures
        for curr_perf_key in perf_keys:
            perf_vals = []
            for curr_perf in all_perfs:
                perf_vals.append(curr_perf[curr_perf_key])
            # compute median and variance for each performance measure.
            curr_median = PatchTester.__get_median(perf_vals)
            if curr_median is not None:
                perfs_median[curr_perf_key] = curr_median
            else:
                log_failure("Unable to compute Median for values:" + str(perf_vals))
                all_ok = False
            curr_variance = PatchTester.__get_variance(perf_vals)
            if curr_variance is not None:
                perfs_variance[curr_perf_key] = curr_variance
            else:
                log_failure("Unable to compute Variance for values:" + str(perf_vals))
                all_ok = False
        if all_ok:
            log_success("Successfully computed performance measures.")
        else:
            log_failure("Error occurred while computing performance values for one or more perf keys.")

        return {"perf": {"median": perfs_median, "variance": perfs_variance}}
