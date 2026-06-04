from os import environ, symlink
from os.path import join, exists, isdir, islink, basename, dirname, expanduser
from glob import iglob
from json import loads, dump
from astropy.io.fits import getval
from shutil import rmtree
from transfer import Remote

class Sync:

    def __init__(self, staging=None, from_sas = None, streams=None, perm=None, sync=None, mjd=None, log_dir=None, process=None, logger=None, verbose=None):
        self.from_sas = from_sas
        self.staging = staging
        self.streams = streams
        self.perm = perm
        self.mjd = mjd
        self.log_dir = log_dir
        self.process = process
        self.logger = logger
        self.verbose = verbose
        self.set_rsync_keywords()
        self.dryrun = ( sync == 'init' )
        self.finalize = ( sync == 'final' )
        self.ready = True
        if self.verbose: print("SYNC> sync: %r, dryrun: %r finalize: %r" % (sync,self.dryrun,self.finalize))
    
    def set_remote(self):
        try: username, hostname, port, ssh_key = environ['TRANSFER_SYNC_USER'], environ['TRANSFER_SYNC_HOST'], environ['TRANSFER_SYNC_PORT'], environ['TRANSFER_SYNC_SSH_KEY']
        except: username, hostname, port, ssh_key = (None, None, None, None)
        ssh_dir = expanduser("~/.ssh")
        key_filename = join(ssh_dir, ssh_key) if ssh_dir and ssh_key else None
        self.remote = Remote(username=username, hostname=hostname, port=port, key_filename=key_filename, verbose=self.verbose) if username and hostname else None
    
    def remote_verify(self):
        if self.ready and self.section and self.mjd and self.remote and self.remote.connected:
            command = "verify_%s" % self.section
            command += " -m %r" % self.mjd
            #command += " -f"
            self.logger.debug(command)
            #self.process.run(command)
            self.remote.exec_command(command)
            if self.remote.return_code:
                self.ready = False
                self.logger.critical("SYNC REMOTE> %s return code %r. Giving up for mjd=%r!" % (section, self.remote.return_code, mjd))
        else:
            self.logger.critical("SYNC REMOTE> not ready")
            
    def set_touch_file(self, filename = None, times = None):
        if self.ready and filename:
            self.touch_file = join(self.staging, self.log_dir, "%r" % self.mjd, filename) if self.staging and self.log_dir and self.mjd else None
            if self.touch_file and not exists(self.touch_file): self.touch_file = None
        else: self.touch_file = None
                
    def set_rsync_keywords(self):
        self.rsync_keywords = "--recursive --links --times --verbose --rsh='ssh'"
        self.rsync_keywords += " --perms" if self.perm else " --no-perms --chmod=ugo=rwX"

    def run_single_rsync_touch(self):
        filename = "transfer-%r.done" % self.mjd if self.ready and self.mjd else None
        self.set_touch_file(filename = filename)
        if self.touch_file:
            dir = "unam:///home/joelbrownstein/transfer/status"
            if self.dryrun and 'dry-run' not in self.rsync_keywords: self.rsync_keywords += " --dry-run"
            command = "rsync {rsync_keywords} "
            command += self.touch_file
            command += " " + join(dir, ".")
            command = command.format(**self.cfg)
            remote_file = join(dir,filename)
            self.process.run(command)
            if not self.process.status: self.logger.info("Touch %r" % remote_file)
            else:
                self.ready = False
                self.logger.info("FAILED touch %r" % remote_file)

    def run_single_rsync(self):
        if self.ready:
            if self.dryrun and 'dry-run' not in self.rsync_keywords: self.rsync_keywords += " --dry-run"
            command = "rsync {rsync_keywords} "
            if self.from_sas: command += "{mjd_dir}/ {remote_path}/{mjd}/"
            else: command += "{remote_path}/{mjd}/ {mjd_dir}/"
            command = command.format(**self.cfg)
            self.process.run(command)
            if self.process.status != 0: self.ready = False

    def run_multiple_rsync(self):
        if self.ready and self.streams:
            if self.from_sas: command = "/bin/ls -1 {mjd_dir}"
            else: command = "{ssh_command} {ssh_config} /bin/ls -1 {path}/{mjd}"
            if self.cfg['folder']: command += "/{folder}"
            command = command.format(**self.cfg)
            self.process.run(command)
            if len(self.process.out) > 0:
                files = [file for file in self.process.out.split("\n") if len(file) > 0]
                streams = []
                for stream_index in range(self.streams):
                    self.cfg['stream_index'] = str(stream_index)
                    stream_files = [files[index] for index in range(len(files)) if index % self.streams == stream_index]
                    self.cfg['stream_filename'] = stream_filename = "{workdir}/{stage}.{section}.{stream_index}.rsync.txt".format(**self.cfg)
                    with open(stream_filename,'w') as stream_file: stream_file.write("\n".join(stream_files)+"\n")
                    command = "rsync {rsync_keywords} --files-from={stream_filename}"
                    if self.from_sas: command += " {mjd_dir}/"
                    else: command += " {remote_path}/{mjd}/"
                    if self.cfg['folder']: command += "{folder}/"
                    if self.from_sas: command += " {remote_path}/{mjd}/"
                    else: command += " {mjd_dir}/"
                    if self.cfg['folder']: command += "{folder}/"
                    command = command.format(**self.cfg)
                    stream_log = stream_filename.replace('.txt','.log')
                    if self.dryrun:
                        streams.append({'command':command ,'outfile':stream_log})
                    else:
                        self.logger.debug(command)
                        outfile = open(stream_log,'w')
                        proc = self.process.open(command, stdout=outfile)
                        streams.append({'proc':proc,'outfile':outfile})
                        self.process.sleep()
                if self.dryrun:
                    stream_file = "{workdir}/{stage}.{section}.rsync.json".format(**self.cfg)
                    with open(stream_file, 'w') as file: dump(streams, file, indent=4)
                else:
                    while any([stream['proc'].poll() is None for stream in streams]):
                        self.logger.debug("Rsync commands still running, going back to sleep.")
                        self.process.sleep(minutes=1)
                    if any([stream['proc'].returncode != 0 for stream in streams]): self.ready = False
                    for stream in streams:
                        try: stream['outfile'].close()
                        except: pass
            else:
                mjd_dir = "{mjd_dir}" if self.from_sas else "{path}/{mjd}"
                mjd_dir = mjd_dir.format(**self.cfg)
                self.logger.info("Directory exists, but no data for %s." % mjd_dir)

    def set_mjd_dir(self, env = None):
        if env:
            boss_section = env.startswith('BOSS') if env else None
            mjd_dir = join('boss',self.section) if boss_section else self.section
            mjd_dir = join(self.staging,mjd_dir,str(self.mjd))
            try:
                self.process.mkdir(mjd_dir)
                self.mjd_dir = mjd_dir
            except Exception as e:
                self.mjd_dir = None
                self.ready = False
        else:
            self.mjd_dir = None

    def set_cfg(self, dir = None, stage = None, options = None):
        if self.ready and options:
            path = 'mirror_path' if self.from_sas else 'path'
            ssh_config = 'ssh_mirror' if self.from_sas else 'ssh_config'
            self.cfg = {
                'section': self.section,
                'workdir': dir,
                'stage': stage,
                'mjd': str(self.mjd),
                'mjd_dir': self.mjd_dir,
                'ssh_command': 'ssh',
                'machine': options.get(self.section,'machine') if options.has_option(self.section,'machine') else None,
                'path': options.get(self.section, path),
                'user': options.get(self.section,'user') if options.has_option(self.section,'user') else None,
                'domain': options.get(self.section,'domain') if options.has_option(self.section,'domain') else None,
                'rsync_keywords': self.rsync_keywords + ' --compress' if options.getboolean(self.section, 'compress') else self.rsync_keywords
            }
            self.cfg['folder'] = options.get(self.section,'folder') if options.has_option(self.section, 'folder') else None
            self.cfg['hostname'] = options.get(self.section,'hostname') if options.has_option(self.section, 'hostname') else "{machine}.{domain}".format(**self.cfg) if self.cfg['machine'] and self.cfg['domain'] else None
            self.cfg['ssh_config'] = options.get(self.section, ssh_config) if options.has_option(self.section, ssh_config) else "{user}@{hostname}".format(**self.cfg) if self.cfg['user'] and self.cfg['hostname'] else None
            self.cfg['remote_path'] = "{ssh_config}:{path}".format(**self.cfg) if self.cfg['ssh_config'] else None
            try:
                self.cfg['ssh_command'] = 'ssh -p %r' % int(options.get(self.section,'port'))
                self.cfg['rsync_keywords'] += ' --rsh="%(ssh_command)s"' % self.cfg
            except: pass

    def set_test(self):
        command = "" if self.from_sas else "{ssh_command} {ssh_config}"
        command += " test -d "
        command = command.format(**self.cfg)
        mjd_dir = "{mjd_dir}" if self.from_sas else "{path}/{mjd}"
        mjd_dir = mjd_dir.format(**self.cfg)
        command += mjd_dir
        self.process.run(command)
        self.test = True if self.process.status == 0 else False if self.process.status == 1 else None
        if self.test:
            self.logger.info("Data found in %s." % mjd_dir)
        elif self.test == False:
            self.logger.info("No data %s - nonexistent path %s." % (self.cfg['section'], mjd_dir))
