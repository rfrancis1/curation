#!/usr/bin/env python2

# Copyright 2015 Google Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Local test runner

Example invocation:
  $ python runner.py ~/google-cloud-sdk
"""
# Python imports
import argparse
import configparser
import os
import sys
import time
import unittest

# Third party imports
try:
    import coverage
except ImportError:
    print(
        'coverage package not found.  Run `pip install -r dev_requirements.txt`'
    )

try:
    import xmlrunner
except ImportError:
    print(
        'xmlrunner package not found.  Run `pip install -r dev_requirements.txt`'
    )


def print_unsuccessful(function, trace, msg_type):
    print(
        '\n======================================================================'
    )
    print(f'{msg_type}:  {function}')
    print(
        '----------------------------------------------------------------------'
    )
    print(trace)
    print(
        '----------------------------------------------------------------------'
    )


def main(test_path, test_pattern, coverage_filepath):
    # Discover and run tests.
    suite = unittest.TestLoader().discover(test_path, pattern=test_pattern)
    all_results = []

    cov = coverage.Coverage(config_file=coverage_filepath)
    cov.start()

    test_type = coverage_filepath.split('_')[1]
    output_file = os.path.join('tests', 'results', 'junit', test_type)
    start_time = time.time()
    for mod_tests in suite:
        if mod_tests.countTestCases():
            runner = xmlrunner.XMLTestRunner(stream=sys.stdout,
                                             output=output_file,
                                             verbosity=2)
            result = runner.run(mod_tests)
            all_results.append(result)

    end_time = time.time()
    cov.stop()

    config = configparser.ConfigParser()
    config.read(coverage_filepath)
    cov_data_file = config.get('run', 'data_file')
    cov_data_filepath = os.path.dirname(cov_data_file)

    try:
        cov.save()
    except OSError:
        # create the directory to save .coverage file to, if needed
        os.makedirs(cov_data_filepath)
        cov.save()

    cov.html_report()
    cov.xml_report()

    run = 0
    errors = []
    failures = []
    for item in all_results:
        run += item.testsRun
        errors.extend(item.errors)
        failures.extend(item.failures)

    print(
        '\n\n\n**********************************************************************'
    )
    print('ALL TEST RESULTS')
    print(
        '**********************************************************************'
    )
    message = f"Ran {run} tests in {end_time - start_time} seconds."

    if errors:
        for err in errors:
            print_unsuccessful(err[0], err[1], 'ERROR')
        message += f"\n{len(errors)} error(s).  "

    if failures:
        for fail in failures:
            print_unsuccessful(fail[0], fail[1], 'FAIL')
        message += f"\n{len(failures)} failure(s)."

    print(message)
    return not errors and not failures


def config_file_path(path):
    try:
        with open(path, 'r'):
            pass
    except OSError:
        # If not given, default to curation/.coveragerc
        path = os.getcwd()
        path = os.path.dirname(path)
        path = os.path.join(path, '.coveragerc')

    return path


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--test-path',
        dest='test_path',
        help='The path to look for tests, defaults to the current directory.',
        default=os.getcwd())
    parser.add_argument(
        '--test-pattern',
        dest='test_pattern',
        help='The file pattern for test modules, defaults to *_test.py.',
        default='*_test.py')
    parser.add_argument(
        '--coverage-file',
        dest='coverage_file',
        required=True,
        help=
        'The path to the coverage file to use.  Defaults to \'curation/.coveragerc\'',
        type=config_file_path,
        default='curation/.coveragerc')

    args = parser.parse_args()

    result_success = main(args.test_path, args.test_pattern, args.coverage_file)

    if not result_success:
        sys.exit(1)
