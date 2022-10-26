# Copyright 2021 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import subprocess
import sys

from enum import Enum

from core.context import Context


class BackupStorage(Enum):
    """
    Available storage for filestream client
    """
    OSS = "OSS"
    SFTP = "SFTP"


class ClientAction(Enum):
    """
    Available action for filestream client
    """
    DownloadOss = "downloadOss"
    UploadOss = "uploadOss"
    DownloadSsh = "DownloadSsh"
    UploadSsh = "uploadSsh"


class FileStreamClient:
    """
    A client to perform stream transmission
    """

    def __init__(self, context: Context, storage: BackupStorage, sink):
        self._client = context.filestream_client()
        self._host_info = context.host_info()
        self._storage = storage
        self._sink = sink
        self._download_action = None
        self._upload_action = None
        self.init_action()

    def upload_from_stdin(self, remote_path, stdin, stderr=sys.stderr, logger=None, is_string_input=False):
        upload_cmd = [
            self._client,
            "--meta.action=" + self._upload_action.value,
            "--meta.sink=" + self._sink,
            "--meta.filename=" + remote_path,
            "--hostInfoFilePath=" + self._host_info
        ]
        if is_string_input and self._storage == BackupStorage.OSS:
            upload_cmd.append("--meta.ossBufferSize=102400")
        if logger:
            logger.info("Upload command: %s" % upload_cmd)
        with subprocess.Popen(upload_cmd, stdin=stdin, stderr=stderr, close_fds=True) as up:
            up.wait()

    def download_to_stdout(self, remote_path, stdout, stderr=sys.stderr, logger=None):
        download_cmd = [
            self._client,
            "--meta.action=" + self._download_action.value,
            "--meta.sink=" + self._sink,
            "--meta.filename=" + remote_path,
            "--hostInfoFilePath=" + self._host_info
        ]
        if logger:
            logger.info("Download command: %s" % download_cmd)
        with subprocess.Popen(download_cmd, stdout=stdout, stderr=stderr, close_fds=True) as dp:
            dp.wait()  # ensure download finished

    def upload_from_file(self, remote, local, stderr=sys.stderr, logger=None):
        """
        upload from src file to dest file

        :param local: local file to upload
        :param remote: remote path to store uploaded file
        :param stderr: redirect stderr
        :param logger: just a logger
        """
        with open(local, "r") as f:
            self.upload_from_stdin(remote_path=remote, stdin=f, stderr=stderr, logger=logger)

    def download_to_file(self, remote, local, stderr=sys.stderr, logger=None):
        """
        download from src file to dest file

        :param remote: remote path of file to download
        :param local: local path to store downloaded file
        :param stderr: redirect stderr
        :param logger: just a logger
        """
        with open(local, 'w') as f:
            self.download_to_stdout(remote_path=remote, stdout=f, stderr=stderr, logger=logger)

    def upload_from_string(self, remote, string, stderr=sys.stderr, logger=None):
        """
        upload from string to remote file
        """
        echo_cmd = [
            "echo",
            string
        ]
        with subprocess.Popen(echo_cmd, stdout=subprocess.PIPE) as pipe:
            self.upload_from_stdin(remote_path=remote, stdin=pipe.stdout, stderr=stderr,
                                   logger=logger, is_string_input=True)

    def init_action(self):
        if self._storage == BackupStorage.OSS:
            self._download_action = ClientAction.DownloadOss
            self._upload_action = ClientAction.UploadOss
        elif self._storage == BackupStorage.SFTP:
            self._download_action = ClientAction.DownloadSsh
            self._upload_action = ClientAction.UploadSsh
        else:
            raise NotImplementedError
