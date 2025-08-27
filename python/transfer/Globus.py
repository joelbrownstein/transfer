from os import chdir, makedirs, environ, listdir
from os.path import join, exists, basename
from json import loads
from re import search
from urllib.request import urlopen
from time import sleep
import tarfile
from collections import OrderedDict

class Globus:

    ext = ['txt', 'log', 'err']
    sync = ['exists', 'size', 'mtime', 'checksum']
    identifier_length = 36
    
    def __init__(self, staging=None, observatory=None, mode=None, mjd=None, sam=None, hpss=None, process=None, logger=None, dir=None, scratch_dir=None, verbose=None):
        self.staging = staging
        self.mjd = mjd
        self.process = process
        self.logger = logger
        self.dir = dir
        self.scratch_dir = scratch_dir
        self.verbose = verbose
        self.set_stage(observatory=observatory,mode=mode)
        self.set_label()
        self.set_user()
        self.set_endpoints(sam=sam, hpss=hpss)
        self.set_ready()
        if self.verbose: print("GLOBUS> ready=%r" % self.ready)
    
    def set_user(self):
        try: self.user = environ['TRANSFER_GLOBUS_USER']
        except Exception as e: self.user = None
    
    def set_file(self):
        self.file = {ext:join(self.dir, "globus.%s.%s" % (self.stage, ext)) for ext in self.ext}

    def set_endpoints(self, sam=None, hpss=None):
        self.set_sas_endpoint(hpss = hpss)
        if sam: self.set_sam_endpoint()
        else: self.sam_endpoint = None
        if hpss: self.set_hpss_endpoint()
        else: self.hpss_endpoint = None
    
    def set_sas_endpoint(self, hpss=None):
        target = self.sas_endpoint = {'endpoint': 'SAS'}
        try: self.sas_endpoint['id'] = environ['TRANSFER_SAS_ENDPOINT']
        except: self.sas_endpoint['id'] = None
        self.set_endpoint_target(target=target)
        self.set_endpoint_base_dir(target=target, hpss=hpss)

    def set_sam_endpoint(self):
        target = self.sam_endpoint = {'endpoint': 'SAM'}
        try: self.sam_endpoint['id'] = environ['TRANSFER_SAM_ENDPOINT']
        except: self.sam_endpoint['id'] = None
        self.set_endpoint_target_force(target=target)
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

    def set_options(self,label=None,sync=None,preserve_mtime=False,verify=False,delete=False,encrypt=False):
        self.options = {'batch': self.file['txt']}
        self.options['sas'] = "%(id)s:%(base_dir)s" % self.sas_endpoint
        self.options['target'] = "%(id)s:%(base_dir)s"
        self.options['target'] %= self.sam_endpoint if self.sam_endpoint else self.hpss_endpoint if self.hpss_endpoint else ''
        self.options['label'] = label if label else self.label
        self.options['sync'] = sync if sync in self.sync else None
        self.options['preserve_mtime'] = preserve_mtime
        self.options['verify'] = verify
        self.options['preserve_mtime'] = preserve_mtime
        self.options['delete'] = delete
        self.options['encrypt'] = encrypt
        mode = []
        if self.options['sync']: mode.append("--sync-level %(sync)s")
        if self.options['preserve_mtime']: mode.append("--preserve-mtime")
        if self.options['encrypt']: mode.append("--encrypt")
        if self.options['verify']: mode.append("--verify-checksum")
        if self.options['delete']: mode.append("--delete")
        if self.options['label']: mode.append("--label=%(label)s")
        self.options['mode'] = " ".join(mode) % self.options
    
    def set_endpoint_target_force(self, target):
        if target:
            if target['id']:
                target['status'] = "active"
                target['active'] = True
            else:
                target['status'] = 'Endpoint ID?'
                target['active'] = None
                
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
                    if self.verbose: print("GLOBUS> %r" % self.process.out)
                else:
                    target['status'] = 'inactive (status code %r)' % self.process.status
                    target['active'] = None
                    self.ready = False
                    if self.verbose: print("GLOBUS> Endpoint Error status code %r (bad syntax)" % self.process.status)
            else:
                target['status'] = 'Endpoint ID?'
                target['active'] = None
        
            if self.verbose:
                print("GLOBUS> %(endpoint)s is %(status)s" % target)

    def set_stage(self, observatory=None, mode=None):
        self.stage = "transfer.%s" % observatory if observatory else "transfer"
        self.stage += ".%s" % mode if mode else ""
        self.stage += ".backup" if self.scratch_dir else ".mirror"

    def set_label(self):
        self.label = self.stage.replace('.','_')
        if self.mjd: self.label += "_%s" % self.mjd

    def set_ready(self):
        sas_ready = self.sas_endpoint and self.sas_endpoint['active'] and self.sas_endpoint['base_dir']
        sam_ready = self.sam_endpoint and self.sam_endpoint['active'] and self.sam_endpoint['base_dir']
        hpss_ready = self.hpss_endpoint and self.hpss_endpoint['active'] and self.hpss_endpoint['base_dir']
        self.ready = sas_ready and (sam_ready or hpss_ready) and self.user and self.dir and exists(self.dir)
        if hpss_ready and self.ready: self.ready = self.scratch_dir and exists(self.scratch_dir)
        self.critical = not self.ready
        if self.ready:
            self.item = []
            self.set_file()
            self.set_active_user()
            self.ready = self.user == self.active_user
            if self.ready:
                if self.verbose: print("GLOBUS> User %s active." % self.user)
            else: print("GLOBUS> Cannot activate user %r because %r is already active." % (self.user, self.active_user))
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
                self.logger.error("GLOBUS> Error status code %r" % self.process.status)
            else:
                lines = [line for line in self.process.out.split("\n") if line]
                self.whoami = lines[0] if len(lines)==1 else None

    def get_target_listing(self, target=None):
        if self.ready and target:
            command = "globus ls %(id)s:%(base_dir)s " % target
            print(command)
            listing=None
            """self.process.run(command)
            if self.process.status:
                listing = None
                self.logger.error("GLOBUS> Error status code %r" % self.process.status)
            else: listing = [line for line in self.process.out.split("\n") if line]"""
        return listing

    def append_target_from_staging(self, resource=None, recursive=None):
        base_dir = join(self.sas_endpoint['base_dir'],'')
        self.target_root = self.staging
        dir = join(self.target_root[len(base_dir):],'') if self.target_root and self.target_root.startswith(base_dir) else None
        if dir is None: dir = self.get_workdir(work='sdsswork')
        boss_section = self.section in ['sos', 'spectro'] if self.section else None
        folder = join('boss',self.section) if boss_section else self.section
        if resource is None: resource = "%s" % self.mjd
        if resource:
            self.target = join(dir, folder, resource) if dir and folder else None
            self.target_path = join(self.target_root, folder, resource) if self.target_root and folder else None
        else:
            self.target = join(dir, folder) if dir and folder else None
            self.target_path = join(self.target_root, folder) if self.target_root and folder else None
        if self.target and recursive: self.target = join(self.target, '')
        if self.target_path and exists(self.target_path): self.append_item(recursive=recursive)
        else: print("GLOBUS> Skipping Nonexistent target path %r" % self.target_path)

    def append_target_from_env(self, resource=None, recursive=None):
        try: self.target_root = environ[self.env]
        except: self.target_root = None
        base_dir = join(self.sas_endpoint['base_dir'],'')
        dir = join(self.target_root[len(base_dir):],'') if self.target_root and self.target_root.startswith(base_dir) else None
        if dir is None: dir = self.get_workdir(work='sdsswork')
        if resource is None: resource = "%s" % self.mjd
        self.target = join(dir, resource) if dir and resource else None
        self.target_path = join(self.target_root, resource) if self.target_root and resource else None
        if self.target and recursive: self.target = join(self.target, '')
        if self.target_path and exists(self.target_path): self.append_item(recursive=recursive)
        else: print("GLOBUS> Skipping Nonexistent target path %r" % self.target_path)

    def append_target_for_backup(self, tarfile=None):
        if tarfile:
            self.target_root = self.scratch_dir
            file = tarfile['file'] if 'file' in tarfile else None
            source = join(self.section, file) if self.section and file else None
            remote = tarfile['remote'] if 'remote' in tarfile else None
            try: hpss_base_dir = join(self.hpss_endpoint['base_dir'], '')
            except: hpss_base_dir = None
            destination = remote[len(hpss_base_dir):] if hpss_base_dir and remote.startswith(hpss_base_dir) else None
            self.target_path = join(self.target_root, self.section) if self.target_root and self.section else None
            if source and remote: self.append_item(source=source, destination=destination)
            else: print("GLOBUS> Skipping Nonexistent target with source=%r destination=%r" % (source, destination))

    def get_workdir(self, work=None):
        if work:
            index = self.target_root.index('%s/' % work) if '/%s/' % work in self.target_root else None
            workdir = join(self.target_root[index:],'') if index is not None else None
        else: workdir = None
        return workdir

    def append_item(self, source=None, destination=None, recursive=False):
        source = source if source else self.target
        destination = destination if destination else self.target
        if self.item is not None:
            if source and destination:
                if self.verbose: print("GLOBUS> Appending target path %r" % self.target_path)
                self.item.append({'source':source, 'destination':destination, 'recursive':recursive})
            else: print("GLOBUS> cannot append no item")

    def commit(self):
        if self.item:
            lines = []
            for item in self.item:
                line = "%(source)s %(destination)s" % item
                if item['recursive']: line += " -r"
                lines.append(line)
            with open(self.file['txt'],'w') as file: file.write("\n".join(lines)+"\n")
            if self.verbose: print("GLOBUS> Create %(txt)s" % self.file)
        else: print("GLOBUS> no items to transfer")

    def set_identifier(self):
        self.identifier = None
        if self.process.out:
            try:
                self.identifier = search('Task ID: (.*?)\n', self.process.out).group(1)
                if self.verbose: print("GLOBUS> Task ID=%r" % self.identifier)
            except AttributeError:print("Cannot find identifier within response=%r" % self.process.out)
        if self.identifier:
            if len(self.identifier)!=self.identifier_length:
                self.identifier=None
                print("Invalid identifier=%r" % self.identifier)

    def submit(self):
        if self.ready and self.item:
            command = "globus transfer %(sas)s %(target)s %(mode)s --batch %(batch)s" % self.options
            if self.verbose:
                print("GLOBUS> %r" % command)
            #self.process.run(command, batch=self.options['batch']) older versions of cli
            self.process.run(command)
            if self.process.status:
                self.ready = False
                self.logger.error("GLOBUS> Error status code %r" % self.process.status)
            else:
                self.set_identifier()
                self.ready = self.identifier is not None
            if not self.ready:
                batch = self.options['batch'] if 'batch' in self.options else  None
                self.logger.critical("GLOBUS> transfer submission failure for command=%r with batch=%r" % (command,batch))


    def wait(self):
        if self.identifier:
            command = "globus task wait %s" % self.identifier
            if self.verbose: print("GLOBUS> Wait...")
            self.process.run(command)
            if self.process.status:
                self.ready = False
                self.logger.error("GLOBUS> Error status code %r" % self.process.status)

    def set_details(self):
        self.details = None
        if self.identifier:
            command = "globus task show %s" % self.identifier
            self.process.run(command)
            if self.process.status:
                self.ready = False
                self.logger.error("GLOBUS> Error status code %r" % self.process.status)
            else: self.details = self.process.out

    def set_status(self):
        self.status = search('Status: (.*?)\n', self.details).group(1) if self.details else None
        if self.status: self.status = self.status.strip()
        if self.verbose: print("GLOBUS> Status=%r" % self.status)
        self.ready = self.status == "SUCCEEDED"
        if not self.ready: self.touch_errfile()

    def write_logfile(self):
        if self.details and self.file['log']:
                if self.verbose: print("GLOBUS> Create %(log)s" % self.file)
                file = open(self.file['log'],'w')
                file.write(self.details)
                file.close()

    def touch_errfile(self):
        if self.details and self.file['err']:
                if self.verbose: print("GLOBUS> Touch %(err)s" % self.file)
                file = open(self.file['log'],'w')
                file.close()


