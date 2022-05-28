help_string = '''
Set of support utilities requiredd by the remote_*.py tools.
'''

import aiofiles
import asyncio
import logging


class RemoteSSHHost:
    '''
    Wraps information and common management tasks for a remote host which will be controlled
    through SSH commands
    '''

    def __init__(self, host_desc):
        '''
        Constructs a remote ssh host object from host description, which is either a simple string
        with just a hostname (FQDN), or JSON object with the following fields:
          host: The hostname (FQDN) of the host, e.g. ec2-18-232-173-175.compute-1.amazonaws.com
          ssh_username: The ssh username to use for connections to the host. Defaulted to the
            default_ssh_username variable below.
          ssh_args: Arguments to pass the the ssh command. Defaulted to the default_ssh_args
            variable below.
        '''

        if (isinstance(host_desc, str)):
            self.host_desc = {'host': host_desc}
        else:
            self.host_desc = host_desc

        #  The 'host' key must exist
        self.host = self.host_desc['host']

        default_ssh_username = 'ubuntu'
        default_ssh_args = '-o StrictHostKeyChecking=no'

        # Populate parameter defaults
        if 'ssh_username' not in self.host_desc:
            self.host_desc['ssh_username'] = default_ssh_username
        if 'ssh_args' not in self.host_desc:
            self.host_desc['ssh_args'] = default_ssh_args

    def __repr__(self):
        return self.host

    async def exec_remote_ssh_command(self, command):
        '''
        Runs the specified command on the cluster host under the SSH credentials configuration above
        '''

        ssh_command = (
            f'ssh {self.host_desc["ssh_args"]} {self.host_desc["ssh_username"]}@{self.host} '
            f'"{command}"')
        logging.info(f'EXEC ({self.host}): {ssh_command}')

        async with aiofiles.tempfile.TemporaryFile() as temp_file:
            ssh_process = await asyncio.create_subprocess_shell(ssh_command, stdout=temp_file,
                                                                stderr=temp_file)
            await ssh_process.wait()

            await temp_file.seek(0)
            async for line in temp_file:
                stripped_line = line.decode('ascii').replace('\n', '')
                logging.info(f'[{self.host}]: {stripped_line}')

            if ssh_process.returncode != 0:
                raise Exception(
                    f'SSH command on host {self.host} failed with code {ssh_process.returncode}')

    async def rsync_files_to_remote(self, source_pattern, destination_path):
        '''
        Uses rsync to copy files matching 'source_pattern' to 'destination_path'
        '''

        rsync_command = (
            f'rsync -e "ssh {self.host_desc["ssh_args"]}" --progress -r -t '
            f'{source_pattern} {self.host_desc["ssh_username"]}@{self.host}:{destination_path}')
        logging.info(f'RSYNC ({self.host}): {rsync_command}')

        async with aiofiles.tempfile.TemporaryFile() as temp_file:
            rsync_process = await asyncio.create_subprocess_shell(rsync_command)
            await rsync_process.wait()

            await temp_file.seek(0)
            async for line in temp_file:
                stripped_line = line.decode("ascii").replace("\n", "")
                logging.info(f'{self.host}: {stripped_line}')

        if rsync_process.returncode != 0:
            raise Exception(
                f'RSYNC command on host {self.host} failed with code {rsync_process.returncode}')
