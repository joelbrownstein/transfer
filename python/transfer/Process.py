from math import log10
from os import environ, getpid, makedirs, unlink
from os.path import join, exists
from sys import exit
from subprocess import Popen, STDOUT
from shlex import split
from tempfile import TemporaryFile
from time import time, sleep

class Process:

    def __init__(self, program=None, mjd=None, logger=None, verbose=False):
        self.program = program if program else "transfer"
        self.mjd = mjd
        self.logger = logger
        self.verbose = verbose
        self.set_ready()

    def run(self, command=None, batch=None, ignore_error=False):
        self.status, self.out, self.err, self.abort = (None, None, None, None)
        if command:
            if self.logger is not None: self.logger.debug(command)
            stdin = open(batch) if batch and exists(batch) else None
            stdout, stderr = (TemporaryFile(), TemporaryFile())
            proc = self.open(command=command, stdin=stdin, stdout=stdout, stderr=stderr)
            tstart = time()
            while proc.poll() is None:
                elapsed = time() - tstart
                if elapsed > 500000:
                    self.abort = "Process still running after more than 5 days!"
                    proc.kill()
                    break
                self.sleep(seconds=10**(int(log10(elapsed))-1))
            stdout.seek(0)
            stderr.seek(0)
            self.status, self.out, self.err = (proc.returncode, stdout.read().decode(), stderr.read().decode())
            stdout.close()
            stderr.close()
            if self.logger is not None:
                if self.status:
                    (self.logger.debug if ignore_error else self.logger.error)("command return code %r" % self.status)
                    if len(self.out) > 0: self.logger.debug("STDOUT:\n" + self.out)
                    if len(self.err) > 0: self.logger.debug("STDERR:\n" + self.err)
                if self.status and self.abort: self.logger.critical(self.abort)
            if self.abort: exit(self.status)
    
    def sleep(self, seconds=None, minutes=None):
        seconds = (seconds if seconds else 0) + (minutes * 60 if minutes else 0)
        sleep(seconds if seconds > 1 else 1)

    def open(self, command=None, stdin=None, stdout=None, stderr=None):
        if stdout is None: stdout = STDOUT
        if stderr is None: stderr = STDOUT
        return Popen(split(str(command)), stdin=stdin, stdout=stdout, stderr=stderr)

    def mkdir(self, path=None, mode=0o775, silent=False):
        if path and not exists(path):
            makedirs(path, mode)
            if self.verbose and not silent: print("PROCESS> CREATE: %r" % path)

    def set_pid_file(self):
        try:
            pid_dir = join('/tmp',environ['USER'],self.program)
            self.mkdir(pid_dir)
            self.pid_file = join(pid_dir, "%r.pid" % int(self.mjd)) if pid_dir and self.mjd else None
        except Exception as e:
            print("PROCESS>: %r" % e)
            self.pid_file = None
    
    def set_pid_from_file(self):
        self.pid = None
        if self.pid_file and exists(self.pid_file):
            with open(self.pid_file,'r') as lines:
                for line in lines:
                    try:
                        self.pid = int(line.strip())
                        break
                    except: pass

    def pid_in_use(self):
        pid_in_use = False
        if self.pid:
            self.run("ps -o command= %r" % self.pid, ignore_error=True)
            pid_in_use = True if self.out and self.program in self.out else None
            if pid_in_use is None and self.pid_file and exists(self.pid_file) : unlink(self.pid_file)
        if pid_in_use and self.verbose: print("PROCESS> Found running pid in %r" % self.pid_file)
        elif self.pid_file:
            if self.verbose: print("PROCESS> Adding new pid in %r" % self.pid_file)
            with open(self.pid_file,'w') as file: file.write("%r\n" % getpid())
        return pid_in_use

    def set_ready(self):
        if self.verbose: print("PROCESS> Checking for running instance of %s" % self.program)
        self.ready = False
        self.set_pid_file()
        self.set_pid_from_file()
        self.ready = not self.pid_in_use()



