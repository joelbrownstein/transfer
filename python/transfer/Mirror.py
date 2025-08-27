from transfer import Process, Logging
from os import chdir, makedirs, environ, listdir
from os.path import join, exists, basename, isdir
from json import loads
from re import search
from urllib.request import urlopen
from time import sleep
import tarfile
from collections import OrderedDict

class Mirror:

    ext = ['txt', 'log', 'err']
    sync = ['exists', 'size', 'mtime', 'checksum']
    identifier_length = 36
    program = 'pando'
    staging = 'mirror_%s' % program
    
    def __init__(self, options=None, location=None, dryrun=None, verbose=None, logger = None):
        self.location = options.location if options else location
        self.dryrun = options.dryrun if options else dryrun
        self.verbose = options.verbose if options else verbose
        self.logger = logger
        self.item = None
        self.set_label()
        self.set_dir()
        self.set_user()
        self.set_logger()
        self.set_process()
        self.set_endpoints()
        self.set_ready()
        self.info_message(message = "ready=%r" % self.ready)
    
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
        self.file = {ext:join(self.dir, "mirror.%s.%s" % (self.program, ext)) for ext in self.ext}

    def set_process(self):  self.process = Process(program = self.program, logger = self.logger, verbose = self.verbose)
    def set_logger(self):
        self.logging = Logging(staging = self.staging, observatory = self.program, dir = self.dir, verbose = self.verbose)
        self.logger = self.logging.logger
        
    def set_user(self):
        try: self.user = environ['TRANSFER_GLOBUS_USER']
        except Exception as e: self.user = None
    
    def set_endpoints(self):
        self.set_sas_endpoint()
        self.set_sam_endpoint()
    
    def set_sas_endpoint(self):
        target = self.sas_endpoint = {'endpoint': 'SAS'}
        try: self.sas_endpoint['id'] = environ['TRANSFER_SAS_ENDPOINT']
        except: self.sas_endpoint['id'] = None
        self.set_endpoint_target(target=target)
        self.set_endpoint_base_dir(target=target)

    def set_sam_endpoint(self):
        target = self.sam_endpoint = {'endpoint': 'SAM'}
        try: self.sam_endpoint['id'] = environ['TRANSFER_SAM_ENDPOINT']
        except: self.sam_endpoint['id'] = None
        self.set_endpoint_target(target=target)
        self.set_endpoint_base_dir(target=target)

    def set_hpss_endpoint(self):
        target = self.hpss_endpoint = {'endpoint': 'HPSS'}
        try: self.hpss_endpoint['id'] = environ['TRANSFER_HPSS_ENDPOINT']
        except: self.hpss_endpoint['id'] = None
        self.set_endpoint_target(target=target)
        self.set_endpoint_base_dir(target=target)

    def set_endpoint_base_dir(self, target=None, hpss=None):
        try: target['base_dir'] = self.scratch_dir if hpss else environ['%(endpoint)s_BASE_DIR' % target]
        except Exception as e: target['base_dir'] = '%r' % e

    def set_endpoint_base_dir_for_sdss5_collection(self, target=None, hpss=None):
        try:
            if hpss:
                target['base_dir'] = self.scratch_dir
                uufs_home_dir = "/uufs/chpc.utah.edu/common/home"
                if target['base_dir'] and target['base_dir'].startswith(uufs_home_dir):
                    target['base_dir'] = target['base_dir'][len(uufs_home_dir):]
            else:
                target['base_dir'] = environ['%(endpoint)s_BASE_DIR' % target]
                if target['endpoint'] == "SAS": target['base_dir'] = "/%s" % basename(target['base_dir'])
        except Exception as e: target['base_dir'] = '%r' % e

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

    def write_batch_file(self):
        if self.item:
            lines = []
            for item in self.item:
                line = "%(source)s %(destination)s" % item
                if item['recursive']: line += " -r"
                lines.append(line)
            with open(self.file['txt'],'w') as file: file.write("\n".join(lines)+"\n")
            self.info_message(message = "Create %(txt)s" % self.file)
        else: self.info_message(message = "no items to transfer")

    def set_options(self,label=None,sync=None,preserve_mtime=False,fail_on_quota_errors=False,verify=False,delete=False,encrypt=False):
        self.options = {'batch': self.file['txt']}
        self.options['sas'] = "%(id)s:%(base_dir)s" % self.sas_endpoint
        self.options['target'] = "%(id)s:%(base_dir)s"
        self.options['target'] %= self.sam_endpoint if self.sam_endpoint else self.hpss_endpoint if self.hpss_endpoint else ''
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
    
    def set_endpoint_target(self, target):
        if target:
            if target['id']:
                command = "globus endpoint is-activated %(id)s" % target
                self.process.run(command)
                if not self.process.status:
                    lines = [line for line in self.process.out.split("\n") if line]
                    response = lines[0] if len(lines)==1 else None
                    active_response = "%(id)s is activated" % target
                    alternate_response = "%(id)s does not require activation" % target
                    inactive_response = "The endpoint is not activated." % target
                    target['status'] = "active" if active_response else "personal endpoint (activation not required)" if alternate_response else "inactive"
                    target['active'] = response == active_response or alternate_response
                elif self.process.status==1:
                    target['status'] = 'inactive (status code %r)' % self.process.status
                    target['active'] = False
                    self.ready = False
                    self.info_message(message = "%r" % self.process.out)
                else:
                    target['status'] = 'inactive (status code %r)' % self.process.status
                    target['active'] = None
                    self.ready = False
                    self.info_message(message = "Endpoint Error status code %r (bad syntax)" % self.process.status)
            else:
                target['status'] = 'Endpoint ID?'
                target['active'] = None
        
            if self.verbose:
                self.info_message(message = "%(endpoint)s is %(status)s" % target)

    def set_label(self):
        parts = [self.program.upper()] if self.program else ['MIRROR']
        if self.location: parts += self.location.split("/")
        self.label = "_".join(parts)

    def set_ready(self):
        sas_ready = self.sas_endpoint and self.sas_endpoint['active'] and self.sas_endpoint['base_dir']
        sam_ready = self.sam_endpoint and self.sam_endpoint['active'] and self.sam_endpoint['base_dir']
        self.ready = sas_ready and sam_ready and self.user and self.dir and exists(self.dir)
        self.critical = not self.ready
        if self.ready:
            self.item = []
            self.set_file()
            self.set_active_user()
            self.ready = self.user == self.active_user
            if self.ready:
                self.info_message(message = "User %s active." % self.user)
            else: self.info_message(message = "Cannot activate user %r because %r is already active." % (self.user, self.active_user))
        if not self.ready: self.item = None
        self.identifier = None

    def set_active_user(self):
        self.set_whoami()
        if self.whoami:
            gid = '@globusid.org'
            self.active_user = self.whoami[:-len(gid)] if self.whoami.endswith(gid) else self.whoami
        else: self.active_user = None
        return self.active_user

    def set_whoami(self):
        if self.ready:
            command = "globus whoami"
            self.process.run(command)
            if self.process.status:
                self.whoami = None
                self.ready = False
                self.error_message(message = "Error status code %r" % self.process.status)
            else:
                lines = [line for line in self.process.out.split("\n") if line]
                self.whoami = lines[0] if len(lines)==1 else None

    def commit(self):
        if self.item:
            lines = []
            for item in self.item:
                line = "%(source)s %(destination)s" % item
                if item['recursive']: line += " -r"
                lines.append(line)
            with open(self.file['txt'],'w') as file: file.write("\n".join(lines)+"\n")
            self.info_message(message = "Create %(txt)s" % self.file)
        else: self.info_message(message = "no items to transfer")

    def set_identifier(self):
        self.identifier = None
        if self.process.out:
            try:
                self.identifier = search('Task ID: (.*?)\n', self.process.out).group(1)
                self.info_message(message = "Task ID=%r" % self.identifier)
            except AttributeError:print("Cannot find identifier within response=%r" % self.process.out)
        if self.identifier:
            if len(self.identifier)!=self.identifier_length:
                self.identifier=None
                print("Invalid identifier=%r" % self.identifier)

    def submit(self):
        if self.ready and self.item:
            command = "globus transfer %(sas)s %(target)s %(mode)s --batch %(batch)s" % self.options
            if self.verbose:
                self.info_message(message = "Command: %r" % command)
            #self.process.run(command, batch=self.options['batch']) older versions of cli
            self.process.run(command)
            if self.process.status:
                self.ready = False
                self.error_message(message = "Error status code %r" % self.process.status)
            else:
                self.set_identifier()
                self.ready = self.identifier is not None
            if not self.ready:
                batch = self.options['batch'] if 'batch' in self.options else  None
                self.critical_message(message = "transfer submission failure for command=%r with batch=%r" % (command,batch))


    def wait(self):
        if self.identifier:
            command = "globus task wait %s" % self.identifier
            self.info_message(message = "Wait...")
            self.process.run(command)
            if self.process.status:
                self.ready = False
                self.error_message(message = "Error status code %r" % self.process.status)

    def set_details(self):
        self.details = None
        if self.identifier:
            command = "globus task show %s" % self.identifier
            self.process.run(command)
            if self.process.status:
                self.ready = False
                self.error_message(message = "Error status code %r" % self.process.status)
            else: self.details = self.process.out

    def set_status(self):
        self.status = search('Status: (.*?)\n', self.details).group(1) if self.details else None
        if self.status: self.status = self.status.strip()
        self.info_message(message = "Status=%r" % self.status)
        self.ready = self.status == "SUCCEEDED"
        if not self.ready: self.touch_errfile()

    def write_logfile(self):
        if self.details and self.file['log']:
                self.info_message(message = "Create %(log)s" % self.file)
                file = open(self.file['log'],'w')
                file.write(self.details)
                file.close()

    def touch_errfile(self):
        if self.details and self.file['err']:
                self.info_message(message = "Touch %(err)s" % self.file)
                file = open(self.file['log'],'w')
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
