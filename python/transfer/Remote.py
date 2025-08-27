from paramiko import SSHClient, AutoAddPolicy
from paramiko.ssh_exception import SSHException
from time import time, sleep
from select import select
from os import remove
import sys

class Remote:
    def __init__(self, username=None, hostname=None, port=None, key_filename=None, timeout=120, verbose=True):
        self.verbose = verbose
        self.username = username
        self.set_hostname(hostname = hostname)
        self.port = port
        self.key_filename = key_filename
        self.timeout = timeout
        self.connected = None
        self.stdout = None
        self.stderr = None
        self.return_code = None
        self.client = SSHClient()
        self.client.set_missing_host_key_policy(AutoAddPolicy())
    
    def set_hostname(self, hostname=None):
        self.hosts = [host.strip() for host in hostname.split(',') if host.strip()] if hostname else None
        self.host_index = 0 if self.hosts else None
        self.hostname = self.hosts[self.host_index] if self.hosts else None
        if self.verbose: print("REMOTE> setting hosts from %r" % self.hosts)
    
    def skip_client_connect(self):
        self.connected = False
        if self.verbose:  print("REMOTE> Skipping host connection")
    
    def client_connect(self):
        if self.username:
            while not self.connected and self.hostname:
                time_start = time()
                self.timed_out = False
                if self.verbose: print("REMOTE> connection attempt[%r] host=%r port=%r key_filename=%r" % (self.host_index, self.hostname, self.port, self.key_filename))
                while not self.connected and not self.timed_out:
                    try:
                        if self.port and self.key_filename: self.client.connect(self.hostname, username=self.username, port=self.port, key_filename=self.key_filename, timeout=self.timeout)
                        else: self.client.connect(self.hostname, username=self.username, timeout=self.timeout)
                        self.connected = True
                        time_elapsed = time() - time_start
                    except Exception as e:
                        self.connected = False
                        time_elapsed = time() - time_start
                        if time_elapsed > self.timeout: self.timed_out = True
                        else: sleep(2)
                if not self.connected and self.timed_out:
                    self.host_index += 1
                    self.hostname = self.hosts[self.host_index] if self.host_index < len(self.hosts) else None
            if self.hostname:
                if self.verbose:
                    if self.timed_out: print("REMOTE> connected to %s [%r] due to timeout > %s seconds" % (self.hostname,self.connected,self.timeout))
                    else: print("REMOTE> connected to %s [%r] after %s seconds elapsed" % (self.hostname,self.connected,time_elapsed))
            else:
                self.connected = False
                if self.verbose:  print("REMOTE> Giving up on host")
        else:
            self.connected = False
            if self.verbose:  print("REMOTE> No username")

    def set_stdout(self, file=None):
        self.stdout = open(file,'w') if file else sys.stdout
    
    def set_stderr(self,file=None):
        self.stderr = open(file,'w') if file else sys.stderr

    def exec_command(self, command, inputlines=[]):
    
        if self.connected:
    
            if not self.stdout: self.set_stdout()
            if not self.stderr: self.set_stderr()
            
            channel = self.client.get_transport().open_session()
            channel.exec_command(command)
            if inputlines:
                for inputline in inputlines: channel.send(inputline)
                channel.shutdown_write()
            
            nbytes = {'out':None,'err':None}
            piped = {'out':False,'err':False}
            response = {'command':'','out':'','err':''}

            while True:
                rl, wl, xl = select([channel],[],[], 0.1)
                if len(rl) > 0:
                    response['out'], nbytes['out'] = self.channel_recv(self.stdout, channel.recv_ready, channel.recv)
                    response['err'], nbytes['err'] = self.channel_recv(self.stderr, channel.recv_stderr_ready, channel.recv_stderr)
                    if nbytes['out']:
                        piped['out'] = True
                        response['command'] += response['out']
                    if nbytes['err']: piped['err'] = True
                    if nbytes['out'] + nbytes['err'] == 0: break
                
            #if not piped['out'] and self.stdout.name!='<stdout>': remove(self.stdout.name)
            #if not piped['err'] and self.stderr.name!='<stderr>': remove(self.stderr.name)


            self.return_code = channel.recv_exit_status()
            if self.verbose: print("REMOTE> %s [RETURN CODE=%r]" % (command,self.return_code))
            channel.close()
            
            self.response = response['command'] if piped['out'] else None

        else: self.response = "Not connected"

    def channel_recv(self, pipe, ready_func, recv_func):
        nbytes = 0
        response = ''
        while ready_func():
            block = recv_func(4096)
            if block:
                try: block = block.decode()
                except: print("REMOTE> block %r" % block)
                nbytes += len(block)
                response += block
                if pipe is not None: pipe.write(block)
        if pipe is not None: pipe.flush()
        return response, nbytes

    def client_close(self):
        if self.connected:
            self.client.close()
            self.connected = False
        if self.verbose: print("REMOTE> closed")
