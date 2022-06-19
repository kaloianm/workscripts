help_string = '''
Set of support utilities requiredd by the remote_*.py tools.
'''

from common import async_start_shell_command


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
        await async_start_shell_command(ssh_command, self.host)

    async def rsync_files_to_remote(self, source_pattern, destination_path):
        '''
        Uses rsync to copy files matching 'source_pattern' to 'destination_path' (on the remote host)
        '''

        rsync_command = (
            f'rsync -e "ssh {self.host_desc["ssh_args"]}" --progress -r -t '
            f'{source_pattern} {self.host_desc["ssh_username"]}@{self.host}:{destination_path}')
        await async_start_shell_command(rsync_command, self.host)

    async def rsync_files_to_local(self, destination_pattern, local_path):
        '''
        Uses rsync to copy files matching 'destination_pattern' (on the remote host) to 'local_path'
        '''

        rsync_command = (
            f'rsync -e "ssh {self.host_desc["ssh_args"]}" --progress -r -t '
            f'{self.host_desc["ssh_username"]}@{self.host}:{destination_pattern} {local_path}')
        await async_start_shell_command(rsync_command, self.host)
