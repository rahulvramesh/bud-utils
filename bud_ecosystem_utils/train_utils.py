import os
import time
import logging
from accelerate.state import PartialState
from ray.job_submission import JobSubmissionClient, JobStatus

from bud_ecosystem_utils.blob import BlobService


class MultiProcessAdapter(logging.LoggerAdapter):
    """
    An adapter to assist with logging in multiprocess.

    `log` takes in an additional `main_process_only` kwarg, which dictates whether it should be called on all processes
    or only the main executed one. Default is `main_process_only=True`.

    Does not require an `Accelerator` object to be created first.
    """
    LAST_LOGGED_AT = None
    BLOB_SERVICE = BlobService()

    @staticmethod
    def _should_log(main_process_only):
        "Check if log should be performed"
        state = PartialState()
        return not main_process_only or (main_process_only and state.is_main_process)

    def log(self, level, msg, *args, **kwargs):
        """
        Delegates logger call after checking if we should log.

        Accepts a new kwarg of `main_process_only`, which will dictate whether it will be logged across all processes
        or only the main executed one. Default is `True` if not passed

        Also accepts "in_order", which if `True` makes the processes log one by one, in order. This is much easier to
        read, but comes at the cost of sometimes needing to wait for the other processes. Default is `False` to not
        break with the previous behavior.

        `in_order` is ignored if `main_process_only` is passed.
        """
        if PartialState._shared_state == {}:
            raise RuntimeError(
                "You must initialize the accelerate state by calling either `PartialState()` or `Accelerator()` before using the logging utility."
            )
        main_process_only = kwargs.pop("main_process_only", True)
        in_order = kwargs.pop("in_order", False)
        blob_key = kwargs.pop("blob_key", self.extra.get("blob_key", None))
        is_last_msg = kwargs.pop("end", False)

        if self.isEnabledFor(level):
            publish_log = is_last_msg or self.LAST_LOGGED_AT is None or time.time() - self.LAST_LOGGED_AT >= int(os.environ.get("LOG_PUBLISH_INTERVAL", 30))
            if self._should_log(main_process_only):
                msg, kwargs = self.process(msg, kwargs)
                self.logger.log(level, msg, *args, **kwargs)
                if publish_log:
                    self.BLOB_SERVICE.upload_file(blob_key, filepath=self.logger.root.handlers[0].baseFilename)
                    self.LAST_LOGGED_AT = time.time()
            elif in_order:
                state = PartialState()
                for i in range(state.num_processes):
                    if i == state.process_index:
                        msg, kwargs = self.process(msg, kwargs)
                        self.logger.log(level, msg, *args, **kwargs)
                        if publish_log:
                            self.BLOB_SERVICE.upload_file(blob_key, filepath=self.logger.root.handlers[0].baseFilename)
                            self.LAST_LOGGED_AT = time.time()
                    state.wait_for_everyone()


def submit_job_to_ray(data, entrypoint=None, runtime_env=None):
    if isinstance(data, dict):
        args = ()
        for key, value in data.items():
            args += (f"--{key}", str(value))
    elif isinstance(data, tuple):
        args = data
    else:
        raise ValueError("data should be of type dict or tuple")
    
    _runtime_env = runtime_env or {}
    blob_provider = os.environ.get("BLOB_PROVIDER", "s3")
    runtime_env = {
        "working_dir": "./",
        "excludes": [".env", ".env.example", "poetry.lock", "run.sh", "node.py", "models.py"],
        # "py_modules": ["modules", "config", "utils"],
        "pip": "requirements.txt",
        "env_vars": {
            "BLOB_PROVIDER": blob_provider,
            "LOG_PUBLISH_INTERVAL": str(os.environ.get("LOG_PUBLISH_INTERVAL", 30))
        }
    }
    if blob_provider == "s3":
        keys = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_BUCKET_NAME"]
    elif blob_provider == "gcp":
        keys = ["GOOGLE_BUCKET_NAME"]
        if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            keys.append("GOOGLE_APPLICATION_CREDENTIALS")
        if os.environ.get("GOOGLE_API_TOKEN"):
            keys.append("GOOGLE_API_TOKEN")
    elif blob_provider == "azure":
        keys = ["AZURE_STORAGE_CONNECTION_STRING", "AZURE_BUCKET_NAME"]
    
    for key in keys:
        runtime_env["env_vars"][key] = os.environ[key]
    runtime_env.update(_runtime_env)

    client = JobSubmissionClient(os.environ["RAY_HEAD_URL"])
    job_id = client.submit_job(
        # Entrypoint shell command to execute
        entrypoint=f"python train.py {' '.join(args)}" if not entrypoint else entrypoint,
        # Path to the local directory that contains the script.py file
        runtime_env=runtime_env,
    )
    return job_id


def stop_ray_job(job_id):
    # TODO: Handle invalid job_id, request failures
    client = JobSubmissionClient(os.environ["RAY_HEAD_URL"])
    status = client.get_job_status(job_id)
    if status in [JobStatus.RUNNING, JobStatus.PENDING]:
        return client.stop_job(job_id)
    return True
