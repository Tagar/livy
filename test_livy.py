#!/usr/bin/env python


#######################################################################################################################

LIVY_ENDPOINT = "https://<redacted>/"

SSL_TRUSTSTORE = "/etc/pki/tls/cert.pem"

SESSION_NAME = 'livy-spark-test-session'    # make this unique per job / per project;
                                             # this will lead to reusing an existing Spark/YARN
                                             # session for sequantial operations


#######################################################################################################################

# import sys
from logging import debug, info, warning, error

import logging
from logging.handlers import RotatingFileHandler

class Logger:

    _format = "%(asctime)-15s %(levelname)-7s pid:%(process)d %(filename)s:%(lineno)-3d %(name)s:%(funcName)-20s %(message)s"
    _log_file_max_bytes = 2 * 1024 * 1024
    _log_file_backup_count = 5

    def __init__(self, log_level='warning', log_to_stdout=True, log_file=None):

        # intialize logging subsystem

        self.logger = logging.getLogger()

        logging_numeric_level = getattr(logging, log_level.upper(), None)
        if not isinstance(logging_numeric_level, int):
            raise ValueError("Invalid log level: %s" % log_level)
        self.logger.setLevel(logging_numeric_level)

        formatter = logging.Formatter(fmt=self._format)

        # logging.basicConfig(level=logging_numeric_level, format=FORMAT)

        if log_file:
            handler = RotatingFileHandler(log_file, maxBytes=self._log_file_max_bytes, backupCount=self._log_file_backup_count)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        if log_to_stdout:
            # dup log to stdout
            stdout_handler = logging.StreamHandler()
            stdout_handler.setFormatter(formatter)
            self.logger.addHandler(stdout_handler)

        if not log_file and not log_to_stdout:
            raise ValueError("You can log to stdout or to a file, or both; none selected")

        logging.captureWarnings(True)


#######################################################################################################################


import requests
from requests_kerberos import HTTPKerberosAuth, REQUIRED
import json
import time
from pprint import pformat


class LivyClient:

    __headers = {'Content-Type': 'application/json'
               , 'Accept': 'application/json'
               , 'X-Requested-By': 'livy-client'
               }

    __default_livy_state_timeout = 60 # seconds
    __default_livy_state_delay = 0.3  # seconds

    last_response = None

    ###

    def __init__(self, livy_endpoint, ssl_verify, mutual_authentication=REQUIRED, sanitize_mutual_error_response=False):

        self.livy_endpoint = livy_endpoint
        self.ssl_verify = ssl_verify
        self.mutual_authentication = mutual_authentication
        self.sanitize_mutual_error_response = sanitize_mutual_error_response

    ###

    def debug_livy_response(self):

        if self.last_response:
            debug(pformat(self.last_response.json()))
        else:
            error(pformat(self.last_response))

    ###

    def make_livy_request(self, method, uri, data=None):

        args = {'headers'   :self.__headers
                , 'auth'    :HTTPKerberosAuth(mutual_authentication=self.mutual_authentication,
                                        sanitize_mutual_error_response=self.sanitize_mutual_error_response)
                , 'verify'  :self.ssl_verify
                }

        if data:
            args.update({'data':json.dumps(data)})

        self.last_response = requests.request(method, self.livy_endpoint + uri, **args)

        self.debug_livy_response()

        return self.last_response

    ###

    def wait_for_livy_state(self, uri, good_states, failure_states=(), timeout_s=None, delay=None):

        if not timeout_s:
            timeout_s = self.__default_livy_state_timeout

        if not delay:
            delay = self.__default_livy_state_delay

        debug(f"Waiting for state {good_states}.. ")

        start_time = time.time()

        while True:

            r_state = self.make_livy_request('get', uri)
            if not r_state:
                error("Failed getting state")
                return r_state

            try:
                state = r_state.json()["state"]
            except:
                error("malformed response? no `state` in response")
                return False

            if state in good_states:
                return r_state

            if state in failure_states:
                # failure state wouldn't lead to a "good" state - safe to fail now
                error(f"Resulted in a failure state {state}")
                return False

            if time.time() - start_time > timeout_s:
                error("timeout reached")
                return False

            time.sleep(delay)


#######################################################################################################################

class LivyClientException(Exception): pass

class GetSessionsException(LivyClientException): pass
class LivyCreateSessionException(LivyClientException): pass
class LivyWaitForIdleSessionException(LivyClientException): pass
class SubmitStatementException(LivyClientException): pass
class LivyWaitForCompletedStatementException(LivyClientException): pass
class StatementParseResponseException(LivyClientException): pass


class LivyClientTestApplication:

    session_id = None
    session_state = None
    r_list_sessions = None

    # this will be used when creating spark session -
    _py_home = '/opt/cloudera/parcels/Anaconda3'

    def __init__(self, livy_endpoint, session_name, ssl_verify=None):

        self.livy_client = LivyClient(livy_endpoint, ssl_verify)

        self.session_name = session_name

    def list_sessions(self):

        info("Getting list of sessions")
        self.r_list_sessions = self.livy_client.make_livy_request('get', 'sessions')

        if not self.r_list_sessions:
            raise GetSessionsException

    def find_our_session(self):

        # check if session with that name already exists

        for session in self.r_list_sessions.json()["sessions"]:

            if session["name"] == self.session_name:

                self.session_id = session["id"]
                self.session_state = session["state"]

                info(f"Found our session to be running under id {self.session_id} in state `{self.session_state}`")

                if self.session_state not in ['idle', 'starting', 'busy']:
                    # other session states - https://livy.incubator.apache.org/docs/latest/rest-api.html#session-state
                    warning("dropping session")
                    r_kill_session = self.livy_client.make_livy_request('delete', f'sessions/{self.session_id}')
                    # ignoring if we couldn't delete the session..
                    self.session_id = None

                break

    def start_new_livy_session(self):

        if self.session_id:
            info(f"Reusing existing session {self.session_id}")
            return

        info("Creating a new session")

        r_create_session = self.livy_client.make_livy_request('post', 'sessions'
                                    , data={'driverMemory': '10g'
                                    , 'driverCores': 3
                                    , 'executorMemory': '14g'
                                    , 'executorCores': 4
                                    , 'name': self.session_name  # also becomes yarn application name
                                    , 'conf': {
                                              "spark.executorEnv.PYSPARK_PYTHON": self._py_home + "/bin/python"
                                            , "spark.executorEnv.LD_LIBRARY_PATH": self._py_home + "/lib"
                                            , "spark.yarn.appMasterEnv.PYSPARK_PYTHON": self._py_home + "/bin/python"
                                            , "spark.yarn.appMasterEnv.LD_LIBRARY_PATH": self._py_home + "/lib"
                                        }
                                    }
                 )
        if not r_create_session:
            raise LivyCreateSessionException

        r_json = r_create_session.json()
        self.session_id = r_json["id"]
        self.session_state = r_json["state"]

        info(f"Created new session {self.session_id}")

    def wait_for_idle_session(self):

        started_waiting_for_session = time.time()

        if not self.livy_client.wait_for_livy_state(uri=f'sessions/{self.session_id}/state'
                , good_states=['idle']
                , failure_states=['shutting_down', 'error', 'dead', 'killed', 'success']
                , timeout_s=120):
            raise LivyWaitForIdleSessionException

        waited_for_session = time.time() - started_waiting_for_session
        debug(f"Waited for session {waited_for_session:.3f}s")

    def submit_pyspark_code(self, code, kind="pyspark"):

        debug(f"Submitting test {kind} code")

        data = {"code": code, "kind": kind}
        r_test_statement = self.livy_client.make_livy_request('post', f'sessions/{self.session_id}/statements', data=data)
        if r_test_statement:
            r_test_statement_id = r_test_statement.json()["id"]
        else:
            error("Failed to submit statement")
            raise SubmitStatementException

        r_completed_statement = self.livy_client.wait_for_livy_state(f'sessions/{self.session_id}/statements/{r_test_statement_id}'
                                                    , ['available']
                                                    , failure_states=['error', 'cancelling', 'cancelled']
                                                    , timeout_s=120
                                                    )
        if not r_completed_statement:
            raise LivyWaitForCompletedStatementException

        r_json = r_completed_statement.json()
        runtime = (float(r_json["completed"]) - float(r_json["started"])) / 1000
        info(f"Output of the execution (statement #{r_test_statement_id}, runtime {runtime:,}s):")

        try:
            info(r_json["output"]["data"]["text/plain"])

        except KeyError:

            if r_json["output"]["status"] == 'error':
                warning(f"Runtime error happend while executing {kind} code remotely.")
                if r_json["output"]["traceback"]:
                    warning("Traceback:")
                    for line in r_json["output"]["traceback"]:
                        warning(line)
                else:
                    # perhaps InterpreterError: Fail to start interpreter
                    error("{}: {}".format(r_json["output"]["ename"], r_json["output"]["evalue"]))

                return False

            else:
                error("Coldn't parse response. Malformed response?")
                raise StatementParseResponseException

        return True

#######################################################################################################################


import sys
from os import environ


def setup_loggers():

    root_logger = Logger(log_level='info', log_to_stdout=True)

    # Since we have changed logging through root logger,
    # suppress some excessive debugging messages
    # coming form requests_kerberos and urllib3 standard modules

    krb_logger = logging.getLogger('requests_kerberos')
    krb_logger.setLevel(logging.ERROR)

    urllib3_logger = logging.getLogger('urllib3')
    urllib3_logger.setLevel(logging.ERROR)


def main_driver():

    setup_loggers()

    app = LivyClientTestApplication(livy_endpoint=LIVY_ENDPOINT
                                    , session_name="{}-{}".format(SESSION_NAME, environ['USER'])
                                    , ssl_verify=SSL_TRUSTSTORE)

    try:
        app.list_sessions()
        app.find_our_session()
        app.start_new_livy_session()
        app.wait_for_idle_session()

        app.submit_pyspark_code(code="""
rdd = sc.parallelize(range(100000))
print("Number of partitions", rdd.getNumPartitions())
print("Number of records", rdd.count())
print("Sum of all elements", rdd.sum())
""")

        # make sure we use Python 3 on spark driver side
        app.submit_pyspark_code("""import sys; print(sys.version_info)""")

    except LivyClientException:
        error("There was a problem with Livy Client Test Application")
        sys.exit(1)



#######################################################################################################################


if __name__ == "__main__":

    main_driver()

