from transfer import Globus_cli, Logging
from os import chdir, makedirs, environ, listdir
from os.path import join, exists, basename, isdir
from collections import OrderedDict
from json import dumps

class Mirror:

    sync = ['exists', 'size', 'mtime', 'checksum']
    label = 'jhu_ceph'
    staging = 'mirror_%s' % label
    
    def __init__(self, options=None, identifier=None, location=None, mjd=None, dryrun=None, verbose=None, logger = None):
        self.identifier = options.identifier if options else identifier
        self.mjd = options.mjd if options else mjd
        self.location = options.location if options else location
        self.dryrun = options.dryrun if options else dryrun
        self.verbose = options.verbose if options else verbose
        self.logger = logger
        self.item = None
        self.set_base_dir()
        self.set_dir()
        self.set_file()
        self.set_user()
        self.set_logger()
        self.set_globus_cli()
        self.info_message(message = "ready=%r for active user=%r" % (self.ready, self.active_user))
    
    def set_base_dir(self):
        self.base_dir = {}
        try:
            self.base_dir['source'] = environ['SAS_BASE_DIR']
            transfer_mirror_dir = "TRANSFER_MIRROR_DR_DIR" if True else "TRANSFER_MIRROR_IPL_DIR" if False else "SAM_BASE_DIR"
            try: self.base_dir['destination'] = environ[transfer_mirror_dir]
            except: self.base_dir = None
        except: self.base_dir = None

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
        if self.dir and self.identifier:
            if getattr(self, 'mjd', None):
                self.file = join(self.dir, "mirror.%s.%d.json" % (self.identifier, self.mjd))
            else:
                self.file = join(self.dir, "mirror.%s.json" % self.identifier)
        else:
            self.file = None

    def set_globus_cli(self):
        self.globus_cli = Globus_cli(logger = self.logger, verbose = self.verbose)
        self.ready = self.globus_cli.ready
        self.set_active_user()
        
    def set_logger(self):
        self.logging = Logging(staging = self.staging, observatory = self.identifier, dir = self.dir, verbose = self.verbose)
        self.logger = self.logging.logger
        
    def set_user(self):
        try: self.user = environ['TRANSFER_GLOBUS_USER']
        except Exception as e: self.user = None

    def append_item(self, label = None, recursive = None):
        if self.item is None: self.item = OrderedDict()
        if not label: label = "item-%03d" % len(self.item)
        if self.base_dir and self.location:
            source = join(self.base_dir['source'], self.location)
            destination = join(self.base_dir['destination'], self.location)
            has_source = exists(source)
            if has_source:
                if recursive is None: recursive = isdir(source)
                item = {'source':source, 'destination':destination, 'recursive':recursive} if has_source else None
                self.item[label] = item
                self.error_message("Appending item=%r [label=%r]" % (item, label))
            else: self.error_message("Nonexistent source path=%r" % source)
        
    def execute_transfer(self):
        if self.item:
            self.globus_cli.execute_transfer(items = self.item, options = self.options)
            self.transfer = self.globus_cli.task
        else:
            self.transfer = None
            self.info_message(message = "no items to transfer")

    def set_options(self, label=None, sync=None, preserve_mtime=False, fail_on_quota_errors=False, verify=False, delete=False, encrypt=False):
        self.options = {}
        self.options['label'] = label if label else self.identifier
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
        if self.globus_cli:
            self.globus_cli.wait()
            self.task = self.globus_cli.task
            self.transfer = self.globus_cli.task  # Keep synchronized for the file writer
            self.status = self.globus_cli.status
            self.ready = self.status == "SUCCEEDED"

    def write_file(self):
        if self.transfer:
            import json
            self.info_message(message = "Create %s" % self.file)
            
            # Extract and deserialize the document mapping safely from the Globus response object
            with open(self.file, 'w') as file:
                task_data = getattr(self.transfer, "data", self.transfer)
                file.write(json.dumps(task_data, indent=4))
                
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
