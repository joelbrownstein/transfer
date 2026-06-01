from transfer import Globus_cli, Logging
from os import environ, makedirs, walk, utime, lstat, readlink, symlink, unlink
from os.path import join, exists, isdir, relpath, getmtime, islink, lexists
from collections import OrderedDict
from json import load, dump, dumps

class Mirror:

    sync_options = ['exists', 'size', 'mtime', 'checksum']
    label = 'jhu_ceph'
    staging = 'mirror_%s' % label
    
    def __init__(self, options=None, identifier=None, location=None, mjd=None, save_manifest=None, manifest_only=None, dryrun=None, verbose=None, logger = None, sync = None):
        self.identifier = options.identifier if options else identifier
        self.mjd = options.mjd if options and hasattr(options, 'mjd') else mjd
        self.location = options.location if options else location
        self.save_manifest = options.save_manifest if options and 'save_manifest' in options else save_manifest
        self.manifest_only = options.manifest_only if options and 'manifes_only' in options else manifest_only
        self.dryrun = options.dryrun if options else dryrun
        self.verbose = options.verbose if options else verbose
        self.logger = logger
        self.item = None
        self.set_sync(sync = sync)
        self.set_public()
        self.set_base_dir()
        self.set_user()
        self.set_dir()
        self.set_file()
        self.set_logger()
        self.set_globus_cli()
    
    def set_public(self):
        self.public = True if self.location and self.location.startswith('dr') and not self.location.startswith('dr20') else False

    def set_sync(self, sync = None):
        if sync:
            self.sync = {'timestamps': [], 'symlinks': [], 'count': {}}
            self.manifest_only = True
        else: self.sync = None
        if self.manifest_only: self.save_manifest = True

    def set_base_dir(self):
        self.base_dir = {}
        try:
            self.base_dir['source'] = environ['SAS_BASE_DIR']
            transfer_mirror_dir = "TRANSFER_MIRROR_DR_DIR" if self.public else "TRANSFER_MIRROR_IPL_DIR"
            try: self.base_dir['destination'] = environ[transfer_mirror_dir]
            except: self.base_dir = None
        except: self.base_dir = None

    def set_dir(self):
        self.dir = {'log': 'TRANSFER_MIRROR_LOG_DIR'}
        if self.save_manifest: self.dir['manifest'] = 'TRANSFER_MIRROR_MANIFEST_DIR'
        if not self.manifest_only: self.dir['task'] = 'TRANSFER_MIRROR_TASK_DIR'
        if self.sync: self.dir['sync'] = 'TRANSFER_MIRROR_SYNC_DIR'
        for dir, env in self.dir.items():
            try: self.dir[dir] = environ[env]
            except: self.dir[dir] = None
            if self.dir and self.dir[dir] and exists(self.dir[dir]):
                if self.location:
                    self.dir[dir] = join(self.dir[dir], self.location)
                    if not exists(self.dir[dir]): makedirs(self.dir[dir])
                self.info_message(message = "%s> %r" % (dir.upper(),self.dir[dir]))
            else:
                self.info_message(message = "nonexistent directory %r" % self.dir[dir])
                self.dir[dir] = None

    def set_file(self):
        self.file = {dir: None for dir in self.dir.keys()}
        for file in self.file.keys():
            prefix = "mirror" if file == "log" else file
            if self.dir and self.dir[file] and self.identifier:
                if getattr(self, 'mjd', None):
                    self.file[file] = join(self.dir[file], "%s.%s.%d.json" % (prefix, self.identifier, self.mjd))
                else:
                    self.file[file] = join(self.dir[file], "%s.%s.json" % (prefix, self.identifier))

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
            mode_word = "%s-only" % mode if mode else 'sync' if self.sync else 'transfer'
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
                if isdir(source):
                    if not source.endswith('/'): source += '/'
                    if not destination.endswith('/'): destination += '/'
                    if recursive is None: recursive = True
                else: recursive = False
                item = {'source':source, 'destination':destination, 'recursive':recursive}
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
            
            location = join(self.location, str(self.mjd)) if self.mjd else self.location                
            source_dir = join(self.base_dir['source'], location)
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
                
            try:
                parts = self.file['manifest'].split('sdsswork/',1)
                location = join('sdsswork', parts[1]) if len(parts) == 2 else None
                self.manifest['source'] = self.file['manifest']
                self.manifest['destination'] = join(environ['TRANSFER_MIRROR_IPL_DIR'], location)
                if self.dir['manifest'] and not exists(self.dir['manifest']): makedirs(self.dir['manifest'])
            except: self.manifest['source'] = self.manifest['destination'] = None

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

    def set_item_for_sync(self):
        self.item = {}
        self.item['location'] = join(self.location, str(self.mjd)) if self.mjd else self.location                
        self.item['directory'] = join(self.base_dir['destination'], self.item['location'] )
        self.item['exists'] = exists(self.item['directory'])
        if self.item['exists']:
            self.info_message("Sync item. Destination directory found: path=%(directory)r" % self.item)
        else:
            self.error_message("Sync aborted. Destination directory not found: path=%(directory)r" % self.item)

    def set_manifest_for_sync(self):
        
        if self.file and 'manifest' in self.file:
            if exists(self.file['manifest']):
                self.info_message("SYNC> manifest path=%(manifest)r" % self.file)
                try:
                    with open(self.file['manifest'], 'r') as file: self.manifest = load(file)
                except Exception as e:
                    self.error_message("Sync aborted: %r" % e)
                    self.manifest = None
            else:
                self.error_message("Sync aborted. Manifest not found: path=%(manifest)r" % self.file)
                self.manifest = None
        else:
            self.error_message("Sync aborted. Manifest not found: file=%r" % self.file)
            self.manifest = None
            
            
    def utime(self, path = None, mtime = None):
        if path and mtime:
            mtimes = ( mtime, mtime )
            try:
                utime(path, mtimes, follow_symlinks = False)
                success = True
            except Exception as e:
                self.error_message("Failed to utime path=%r: %r" % (path, e))
                success = False
            self.sync['timestamps'].append("touch -h -d @%r %s #success=%r" % (mtime, path, success))
        else: success = None
        return success

    def finalize_symlink(self, path=None, target=None, mtime=None, success=None):
        if path and target and mtime:
            status = 'success' if success else 'fail'
            self.sync['symlinks'].append("ln -s %s %s #success=%r" % (target, path, success))
            self.sync['count']['symlinks'][status] += 1
            timestamp_ok = self.utime(path = path, mtime = mtime) if success else True
            if timestamp_ok ok:
                self.error_message("Failed to sync symlink timestamp path=%r [mtime=%r]" % (path, mtime))
            
    def sync_symlinks(self):
        if self.item and self.item['exists'] and self.manifest:
            symlinks = self.manifest['symlinks'] if 'symlinks' in self.manifest else None
            if symlinks is not None:
                self.info_message("Restoring symlinks...")
                self.sync['count']['symlinks'] = {'success': 0, 'fail': 0}
                for location, link in symlinks.items():
                    path = join(self.item['directory'], location)
                    target, mtime = ( link['target'], link['mtime'] )
                    if lexists(path):
                        if islink(path) and readlink(path) == target:
                            self.info_message("Link already exists for target=%r to path=%r" % (target, path))
                            self.finalize_symlink(path=path, target=target, mtime=mtime, success=True)
                        else:
                            try:
                                unlink(path)
                                symlink(target, path)
                                self.finalize_link(path=path, target=target, mtime=mtime, success=True)
                            except Exception as e:
                                self.error_message("Failed to link target=%r to path=%r: %r" % (target, path, e))
                                self.finalize_symlink(path=path, target=target, mtime=mtime, success=False)
                    else:
                        symlink(target, path)
                        self.finalize_symlink(path=path, target=target, mtime=mtime, success=True)
                self.info_message(f"Sync symlinks complete. Success count=%(success)r, Fail count=%(fail)r" % self.sync['count']['symlinks'])
            else:
                self.error_message(f"Sync symlinks failed.  symlinks not in manifest=%r" % self.manifest)

    def sync_timestamps(self):
        if self.item and self.item['exists'] and self.manifest:
            locations = self.manifest['locations'] if 'locations' in self.manifest else None
            if locations is not None:
                self.info_message("Restoring timestamps...")
                self.sync['count']['timestamps'] = {'success': 0, 'fail': 0}
                for location, mtime in locations.items():
                    path = join(self.item['directory'], location) if location else self.item['directory']
                    if exists(path):
                        if isdir(path): success = self.utime(path = path, mtime = mtime)
                        else:
                            self.error_message("Failed to sync timestamp path=%r is not a directory" % path)
                            success = False
                    else:
                        self.error_message("Failed to sync timestamp path=%r does not exist" % path)
                        success = False
                    if success: self.sync['count']['timestamps']['success'] += 1
                    else: self.sync['count']['timestamps']['fail'] += 1
                self.info_message(f"Sync timestamp complete. Success count=%(success)r, Fail count=%(fail)r" % self.sync['count']['timestamps'])
            else:
                self.error_message(f"Sync timestamp failed.  locations not in manifest=%r" % self.manifest)
        
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
        self.options['sync'] = sync if sync in self.sync_options else self.sync_options[0]
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

    def write_sync_file(self):
        if self.sync:
            self.info_message(message = "Create %(sync)s" % self.file)
            with open(self.file['sync'], 'w') as file:
                file.write(dumps(self.sync, indent=4))
                
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
