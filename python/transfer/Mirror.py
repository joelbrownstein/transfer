from transfer import Globus_cli, Logging
from os import environ, makedirs, walk, utime, lstat, readlink, symlink, unlink
from os.path import join, exists, basename, isdir, relpath, getmtime, islink, lexists
from collections import OrderedDict
from json import load, dump, dumps

class Mirror:

    sync = ['exists', 'size', 'mtime', 'checksum']
    label = 'jhu_ceph'
    staging = 'mirror_%s' % label
    
    def __init__(self, options=None, identifier=None, location=None, mjd=None, save_manifest=None, manifest_only=None, dryrun=None, verbose=None, logger = None):
        self.identifier = options.identifier if options else identifier
        self.mjd = options.mjd if options and hasattr(options, 'mjd') else mjd
        self.location = options.location if options else location
        self.save_manifest = options.save_manifest if options else save_manifest
        self.manifest_only = options.manifest_only if options else manifest_only
        if self.manifest_only: self.save_manifest = True
        self.dryrun = options.dryrun if options else dryrun
        self.verbose = options.verbose if options else verbose
        self.logger = logger
        self.item = None
        self.set_base_dir()
        self.set_user()
        self.set_dir()
        self.set_file()
        self.set_logger()
        self.set_globus_cli()
    
    def set_base_dir(self):
        self.base_dir = {}
        try:
            self.base_dir['source'] = environ['SAS_BASE_DIR']
            transfer_mirror_dir = "TRANSFER_MIRROR_DR_DIR" if True else "TRANSFER_MIRROR_IPL_DIR" if False else "SAM_BASE_DIR"
            try: self.base_dir['destination'] = environ[transfer_mirror_dir]
            except: self.base_dir = None
        except: self.base_dir = None

    def set_dir(self):
        self.dir = {'log': 'TRANSFER_MIRROR_LOG_DIR'}
        if self.save_manifest: self.dir['manifest'] = 'TRANSFER_MIRROR_MANIFEST_DIR'
        if not self.manifest_only: self.dir['task'] = 'TRANSFER_MIRROR_TASK_DIR'
        for dir, env in self.dir.items():
            try: self.dir[dir] = environ[env]
            except: self.dir[dir] = None
            if self.dir and self.dir[dir] and exists(self.dir[dir]):
                if self.location:
                    self.dir[dir] = join(self.dir[dir], self.location)
                    if not exists(self.dir[dir]): makedirs(self.dir[dir])
                self.info_message(message = "dir=%r to %r" % (dir,self.dir[dir]))
            else:
                self.info_message(message = "nonexistent directory %r" % self.dir[dir])
                self.dir[dir] = None

    def set_file(self):
        self.file = {dir: None for dir in self.dir.keys()}
        for file in self.file.keys():
            if self.dir and self.dir[file] and self.identifier:
                if getattr(self, 'mjd', None):
                    self.file[file] = join(self.dir[file], "mirror.%s.%d.json" % (self.identifier, self.mjd))
                else:
                    self.file[file] = join(self.dir[file], "mirror.%s.json" % self.identifier)

    def set_globus_cli(self):
        if not self.manifest_only:
            self.globus_cli = Globus_cli(logger = self.logger, verbose = self.verbose)
            self.ready = self.globus_cli.ready
            self.set_active_user()
            self.info_message(message = "ready=%r for active user=%r" % (self.ready, self.active_user))
        else:
            self.globus_cli = None
            self.ready = True
            self.active_user = None
            self.info_message(message = "ready=%r for manifest_only=%r" % (self.ready, self.manifest_only))
        
    def set_logger(self):
        print("LOGGING> needed=%r" % True if not self.logger else False)
        if not self.logger:
            mode = "manifest" if self.manifest_only else None
            mode_word = "%s-only" % mode if mode else 'transfer'
            print("LOGGING> staging=%r [%s mode]" % (self.staging, mode_word))
            self.logging = Logging(staging = self.staging, observatory = self.identifier, dir = self.dir['log'], mjd = self.mjd, mode = mode, verbose = self.verbose)
            self.logger = self.logging.logger
        
    def set_user(self):
        try: self.user = environ['TRANSFER_GLOBUS_USER']
        except Exception as e: self.user = None

    def append_item(self, label = None, recursive = None):
        if self.item is None: self.item = OrderedDict()
        if not label:
            if self.mjd: label = "mjd-%r" % self.mjd
            else: label = "item-%03d" % len(self.item)
        if self.base_dir and self.location:
            source = join(self.base_dir['source'], self.location)
            destination = join(self.base_dir['destination'], self.location)
            if self.mjd:
                mjd = str(self.mjd)
                source = join(source,mjd)
                destination = join(destination,mjd)
            has_source = exists(source)
            if has_source:
                if recursive is None: recursive = isdir(source)
                item = {'source':source, 'destination':destination, 'recursive':recursive} if has_source else None
                self.item[label] = item
            else: self.error_message("Nonexistent source path=%r" % source)

    def set_manifest(self):
        """
        PRE-FLIGHT (runs on source): Scans the local directory tree, calculates relative
        paths and their Mtime, dumps a JSON file to a designated manifest directory,
        and appends it to the Globus transfer list to sync alongside the data.
        """
        if self.save_manifest:
            if not self.base_dir or not self.location or self.item is None: return
            
            loc = self.location
            if getattr(self, 'mjd', None) and not loc.endswith(str(self.mjd)):
                loc = join(loc, str(self.mjd))
                
            source_dir = join(self.base_dir['source'], loc)
            if not exists(source_dir): return
            
            self.info_message("Pre-flight: Getting directory timestamps and symlinks...")
            self.manifest = {'source': None, 'destination': None, 'locations': {'': getmtime(source_dir)}, 'symlinks': {}}

            for root, dirs, files in walk(source_dir):
                for entity in dirs + files:
                    path = join(root, entity)
                    location = relpath(path, source_dir)
                    
                    if islink(path):
                        self.manifest['symlinks'][location] = {
                            'target': readlink(path),
                            'mtime': lstat(path).st_mtime
                        }
                    elif entity in dirs:
                        self.manifest['locations'][location] = getmtime(path)
                
            # Write out to the designated environmental folder (fallback to log dir)
            local_manifest_dir = environ.get('TRANSFER_MIRROR_MANIFEST_DIR', self.dir)
            if local_manifest_dir and not exists(local_manifest_dir): makedirs(local_manifest_dir)
            
            self.manifest['source'] = self.file['manifest']
            filename = basename(self.file['manifest'])
            dest_manifest_dir = environ.get('TRANSFER_MIRROR_DEST_MANIFEST_DIR', local_manifest_dir)
            self.manifest['destination'] = join(dest_manifest_dir, filename)
            
            with open(self.manifest['source'], 'w') as file:
                dump(self.manifest, file, indent=4)
            self.info_message("Pre-flight Manifest packaged: %(source)s" % self.manifest)
            
            label = "manifest-%r" % self.mjd if self.mjd else "manifest"
            self.item[label] = {
                'source': self.manifest['source'],
                'destination': self.manifest['destination'],
                'recursive': False
            }
        else: self.manifest = None

    def apply_timestamp_manifest(self):
        """
        POST-FLIGHT (JHU side): Reads the transferred JSON manifest from the 
        environmental directory and applies the exact timestamps via os.utime.
        """
        local_manifest_dir = environ.get('TRANSFER_MIRROR_MANIFEST_DIR', self.dir)
        manifest_file = join(local_manifest_dir, "manifest.%s.json" % self.identifier)
        
        if not exists(manifest_file):
            self.error_message("Timestamp sync aborted. Manifest not found: %s" % manifest_file)
            return
            
        self.info_message("Restoring directory timestamps from manifest: %s" % manifest_file)
        with open(manifest_file, 'r') as f:
            manifest = load(f)
            
        loc = self.location
        if getattr(self, 'mjd', None) and not loc.endswith(str(self.mjd)):
            loc = join(loc, str(self.mjd))
            
        dest_dir = join(self.base_dir['destination'], loc)
        
        success_count, error_count = 0, 0
        for rel_p, mtime in manifest.items():
            target_dir = join(dest_dir, rel_p) if rel_p else dest_dir
            if exists(target_dir) and isdir(target_dir):
                try:
                    utime(target_dir, (mtime, mtime))
                    success_count += 1
                except Exception as e:
                    self.error_message("Failed to utime %s: %s" % (target_dir, e))
                    error_count += 1
        
        self.info_message(f"Directory timestamp restoration complete. Succeeded: {success_count}, Failed: {error_count}")
        
    def execute_transfer(self):
        if not self.manifest_only:
            if self.item:
                self.globus_cli.execute_transfer(items = self.item, options = self.options)
                self.transfer = self.globus_cli.task
            else:
                self.transfer = None
                self.info_message(message = "no items to transfer")
        else:
            self.transfer = None
            self.info_message(message = "skipping transfer (save manifest only)")

    def set_options(self, label=None, sync=None, preserve_mtime=False, fail_on_quota_errors=False, verify=False, delete=False, encrypt=False):
        self.options = {}
        self.options['label'] = label if label else self.identifier
        self.options['sync'] = sync if sync in self.sync else self.sync[0]
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
            self.transfer = self.globus_cli.task  
            self.status = self.globus_cli.status
            self.ready = self.status == "SUCCEEDED"

    def write_task_file(self):
        if self.transfer:
            self.info_message(message = "Create %(task)s" % self.file)
            with open(self.file['task'], 'w') as file:
                task_data = getattr(self.transfer, "data", self.transfer)
                file.write(dumps(task_data, indent=4))
                
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
