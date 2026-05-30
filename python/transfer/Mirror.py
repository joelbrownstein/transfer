from transfer import Globus_cli, Logging
from os import chdir, makedirs, environ, listdir
from os.path import join, exists, basename, isdir
from json import loads
from re import search
from urllib.request import urlopen
from time import sleep
import tarfile
from collections import OrderedDict

class Mirror:

    sync = ['exists', 'size', 'mtime', 'checksum']
    label = 'jhu_ceph'
    staging = 'mirror_%s' % label
    
    def __init__(self, options=None, location=None, dryrun=None, verbose=None, logger = None):
        self.location = options.location if options else location
        self.dryrun = options.dryrun if options else dryrun
        self.verbose = options.verbose if options else verbose
        self.logger = logger
        self.item = None
        self.set_dir()
        self.set_user()
        self.set_logger()
        self.set_globus_cli()
        self.info_message(message = "ready=%r for active user=%r" % (self.ready, self.active_user))
    
    def set_dir(self):
        try: self.dir = environ['SAM_LOGS_DIR']
        except: self.dir = None
        if self.dir and exists(self.dir):
            if self.location:
                self.dir = join(self.dir, self.location)
                if not exists(self.dir): makedirs(self.dir)
            self.info_message(message = "logging to %r" % self.dir)
        else:
            self.info_message(message = "nonexistent directory %r" % self.dir)
            self.dir = None

    def set_file(self):
        self.file = join(self.dir, "mirror.%s.json" % self.label) if self.dir and self.label else None

    def set_globus_cli(self):
        self.globus_cli = Globus_cli()
        self.ready = self.globus_cli.ready
        self.set_active_user()
        
    def set_logger(self):
        self.logging = Logging(staging = self.staging, observatory = self.label, dir = self.dir, verbose = self.verbose)
        self.logger = self.logging.logger
        
    def set_user(self):
        try: self.user = environ['TRANSFER_GLOBUS_USER']
        except Exception as e: self.user = None

    def set_item(self):
        base_dir = self.sas_endpoint['base_dir'] if self.sas_endpoint else None
        if base_dir and self.location:
            path = join(base_dir, self.location)
            if exists(path):
                self.append_item(recursive = isdir(path))
            else: self.error_message("Nonexistent path=%r" % path)
        else: self.item = None
        
    def append_item(self, recursive=False):
        if self.location and self.item is not None:
            item = {'source':self.location, 'destination':self.location, 'recursive':recursive}
            self.item.append(item)
            self.info_message(message = "Item %r" % self.item)
        else: self.error_message("Cannot append no location to item")

    def execute_transfer(self):
        if self.item:
            self.globus_cli.execute_transfer(items = self.item, options = self.options)
        else: self.info_message(message = "no items to transfer")

    def set_options(self,label=None,sync=None,preserve_mtime=False,fail_on_quota_errors=False,verify=False,delete=False,encrypt=False):
        self.options = {}
        self.options['label'] = label if label else self.label
        self.options['sync'] = sync if sync in self.sync else None
        self.options['preserve_mtime'] = preserve_mtime
        self.options['fail_on_quota_errors'] = fail_on_quota_errors
        self.options['verify'] = verify
        self.options['preserve_mtime'] = preserve_mtime
        self.options['delete'] = delete
        self.options['encrypt'] = encrypt
        mode = []
        if self.options['sync']: mode.append("--sync-level %(sync)s")
        if self.options['preserve_mtime']: mode.append("--preserve-mtime")
        if self.options['encrypt']: mode.append("--encrypt")
        if self.options['fail_on_quota_errors']: mode.append("--fail-on-quota-errors")
        if self.options['verify']: mode.append("--verify-checksum")
        if self.options['delete']: mode.append("--delete")
        if self.options['label']: mode.append("--label=%(label)s")
        self.options['mode'] = " ".join(mode) % self.options
    
    def set_active_user(self):
        if self.ready:
            self.globus_cli.set_whoami()
            whoami = self.globus_cli.whoami
            try:
                self.active_user = "%(username)s <%(email)s>" % whoami if whoami else None
            except: self.active_user = None
        else: self.active_user = None

    def wait(self):
        self.globus_cli.wait()
        self.task = self.globus_cli.task
        self.status = self.globus_cli.status
        self.ready = self.status == "SUCCEEDED"

    def write_file(self):
        if self.task:
            self.info_message(message = "Create %s" % self.file)
            file = open(self.file,'w')
            file.write(self.task)
            file.close()
                
    def done(self):
        self.info_message(message = "Done!")
        
    def info_message(self, message = None):
        if message:
            if self.logger: self.logger.info("MIRROR> %s" % message)
            elif self.verbose: print(message)

    def error_message(self, message = None):
        if message:
            if self.logger: self.logger.error("MIRROR> %s" % message)
            elif self.verbose: print(message)

    def critical_message(self, message = None):
        if message:
            if self.logger: self.logger.critical("MIRROR> %s" % message)
            elif self.verbose: print(message)
