from os import chdir, makedirs, environ, listdir
from os.path import join, exists, basename
from pathlib import Path
import rclone

class Rclone:

    observatories = {'N': 'apo', 'S': 'lco'}
    
    def __init__(self, options = None, observatory = None, staging=None, env=None, mjd=None, logger=None, dir=None, verbose=None, dryrun=None):
        self.env = options.env if options else env
        self.dir = options.dir if options else dir
        self.verbose = options.verbose if options else verbose
        self.dryrun = options.dryrun if options else dryrun
        self.set_observatory(observatory = observatory)
        self.set_staging(staging = staging)
        self.mjd = options.mjd if options else mjd
        self.logger = logger
        self.set_stage()
        self.set_file()
        self.set_label()
        self.set_rclone()
        self.set_remotes()
        self.set_ready()
        self.set_path()
        self.set_item()
    
    def set_observatory(self, observatory = None):
        if observatory: self.observatory = observatory
        else:
            key = self.env[-1] if self.env and len(self.env)>1 else None
            self.observatory = self.observatories[key] if key in self.observatories else None

    def set_staging(self, staging = None):
        if staging: self.staging = staging
        else:
            try: self.staging = environ["%s_STAGING_DATA" % self.observatory.upper() if self.observatory else None]
            except: self.staging = None
    
    def set_path(self):
        try:
            self.path = environ[self.env] if self.env else None
            if self.path: self.path = join(self.path, str(self.mjd))
        except: self.path = None
        
    def set_rclone(self):
        config = join(Path.home(), '.config', 'rclone', 'rclone.conf')
        if exists(config):
            try:
                with open(config) as file: config = file.read()
            except: config = None
            self.rclone = rclone.with_config(config) if config else None
        else:
            self.info_message(message = "Nonexisent %r" % config)
            self.rclone = None
    
    def set_stage(self):
        self.stage = "transfer.%s.mirror" % self.observatory if self.observatory else "transfer"

    def set_label(self):
        self.label = self.stage.replace('.','_')
        if self.mjd: self.label += "_%s" % self.mjd

    def set_ready(self):
        self.ready = self.rclone is not None and self.remotes['ready']
        
    def set_item(self):
        self.item = [] if self.ready else None
        if self.env: self.append_item()

    def set_remotes(self):
        self.remotes = self.rclone.listremotes() if self.rclone else None
        if self.remotes:
            try:
                out = self.remotes['out'].decode().split("\n") if self.remotes else None
                out = [o for o in out if o]
                self.remotes['user'] = out[0] if len(out)==1 else None
            except: self.remotes['user'] = None
            self.remotes['ready'] = True if self.remotes['code']==0 and not self.remotes['error'] and self.remotes['user'] is not None else False
        else: self.remotes = {'ready': False, 'user': None}

    def set_remote(self):
        self.remote = self.remotes['out'] if self.remotes else None
        self.remote = self.remote.split("\n")
        self.remote = self.remote[0] if len(self.remote) == 1 else None
        
    def append_item(self):
        self.set_path()
        source = self.path if self.path and exists(self.path) else None
        destination = "%s%s-%r" % (self.remotes['user'], self.env, self.mjd) if self.env and self.mjd else None
        if self.item is not None:
            if source and destination:
                item = {'source':source, 'destination':destination}
                self.item.append(item)
                self.info_message(message = "Appending item=%r" % item)
            #else: self.error_message(message = "Skipping item for path=%r" % self.path)
        
    def mkdir(self, flags = None):
        if self.rclone and self.ready and self.item:
            if flags is None: flags = []
            for item in self.item:
                args = [item['destination']] + flags
                item['mkdir'] = self.rclone.run_cmd(command="mkdir", extra_args=args)

    def ls(self, flags = None):
        if self.rclone and self.ready:
            if flags is None: flags = []
            for item in self.item:
                args = [item['destination']] + flags
                item['listing'] = self.rclone.run_cmd(command="ls", extra_args=args)

    def copy(self, flags = None):
        if self.rclone and self.ready:
            if flags is None: flags = []
            for item in self.item:
                args = [item['destination']] + flags
                item['copy'] = self.rclone.copy(item['source'], item['destination'])

    def sync(self, flags = None):
        if self.rclone and self.ready:
            if flags is None: flags = []
            for item in self.item:
                args = [item['destination']] + flags
                item['sync'] = self.rclone.sync(item['source'], item['destination'])

    def set_details(self):
        self.details = None
        if self.item:
            self.details = []
            for item in self.item:
                item['details'] = "%(destination)s" % item
                mkdir, copy, listing = item['mkdir'], item['copy'], item['listing']
                rows = [row for row in listing['out'].decode().split("\n") if row]
                item['count'] = len(rows)
                item['details'] += " [mkdir:%(code)r]" % mkdir
                item['details'] += " [copy:%(code)r]" % copy
                item['details'] += " [ls:%(code)r]" % listing
                item['details'] += " (%(count)r files)" % item
                self.details.append(item['details'])
                self.details += rows
                self.details.append('_'*80)
            self.details = "\n".join(self.details)

    def write_logfile(self):
        if self.details and self.file:
                self.info_message(message = "Create %s" % self.file)
                file = open(self.file,'w')
                file.write(self.details)
                file.close()

    def set_file(self):
        self.file = join(self.dir, "rclone.%s.log" % self.stage) if self.dir and self.stage else None

    def done(self):
        self.info_message(message = "Done!")
        
    def info_message(self, message = None):
        if message:
            if self.logger: self.logger.info("RCLONE> %s" % message)
            elif self.verbose: print(message)

    def error_message(self, message = None):
        if message:
            if self.logger: self.logger.error("RCLONE> %s" % message)
            elif self.verbose: print(message)

    def critical_message(self, message = None):
        if message:
            if self.logger: self.logger.critical("RCLONE> %s" % message)
            elif self.verbose: print(message)
