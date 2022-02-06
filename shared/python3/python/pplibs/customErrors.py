import boto3
from json import dumps
import pickle
_SNS = boto3.client('sns')

class ProPairError(Exception):
    def __init__(self, err, error_type="NO_REFERER", exec_info="", stack=""):

        # Call the base class constructor with the parameters it needs
        super(ProPairError, self).__init__(err)

        if isinstance(err, str):
            self.message = err.replace("\"", "")
        else:
            self.message = str(err).replace("\"", "")

        if isinstance(exec_info, str):
            self.exec_info = exec_info.replace("\"", "'")
        else:
            self.exec_info = str(exec_info).replace("\"", "'")
        self.stack = stack.replace("\"", "")
        # Now for your custom code...
        self.referer = error_type
        self.name = error_type
        self.error_type = "PythonException"

def report_to_rollbar(topic, py_err):
    rollbar_error = {}
    if not isinstance(py_err, ProPairError):
        try:
            if isinstance(py_err, str):
                rollbar_error = json.dumps(py_err)
            else:
                if hasattr(py_err, "args"):
                    rollbar_error = py_err.__dict__
                    rollbar_error["message"] = py_err.args[0]
        except Exception as e:
            rollbar_error["message"] = "No error format"
    else:
        rollbar_error = py_err.__dict__
    rollbar_error["message"] = py_err.message if hasattr(py_err, "message") else ""
    rollbar_error = dumps(rollbar_error)
    response = _SNS.publish(
        TopicArn=topic, 
        Subject="Production Error in Recommend Lambda",   
        Message=rollbar_error
    )
    print("Response: {}".format(response))
