from os import environ, symlink, utime
from os.path import join, exists, isdir, islink, basename, dirname
from glob import iglob
from json import loads
from astropy.io.fits import getval
from shutil import rmtree

class Copy:

    def __init__(self, staging=None, source=None, destination=None, mjd=None, log_dir=None, process=None, logger=None, verbose=None):
        self.staging = staging
        self.mjd = mjd
        self.log_dir = log_dir
        self.process = process
        self.logger = logger
        self.verbose = verbose
        self.set_base_dir()
        self.set_source(path = source)
        self.set_destination(path = destination)
        self.ready = True
    
    def set_base_dir(self):
        try: self.base_dir = environ['SAS_BASE_DIR']
        except: self.base_dir = None

    def set_source(self, path=None, env=None, section=None):
        boss_section = env.startswith('BOSS') if env else None
        folder = join('boss',section) if boss_section else section
        self.source = path if path else join(self.staging, folder) if self.staging and folder else None
        self.set_ready()
    
    def set_destination(self, path=None, env=None, partition=None):
        try:
            self.destination = path if path else environ[env] if env else None
            if partition and self.destination and self.destination.startswith(self.base_dir) and self.base_dir and not self.base_dir.endswith(partition):
                partition_dir = join(dirname(self.base_dir), partition) if self.base_dir and partition else None
                self.destination = self.destination.replace(self.base_dir, partition_dir, 1)
        except: self.destination = None
        self.set_ready()
    
    def set_ready(self):
        self.ready = False
        if self.source and self.destination:
            if not isdir(self.source): self.logger.critical("Could not find source directory {0}".format(self.source))
            elif not isdir(self.destination): self.logger.critical("Could not find destination directory {0}".format(self.destination))
            else: self.ready = True

    def copy_mjd(self):
        if self.ready:
            command = "rsync --archive --verbose {source}/{mjd}/ {destination}/{mjd}/".format(source=self.source,destination=self.destination,mjd=self.mjd)
            if self.verbose: print("COPY> %r" % command)
            self.process.run(command)
            if self.process.status:
                self.ready = False
                self.logger.critical("Error detected while copying {source}/{mjd}.".format(source=self.source,mjd=self.mjd))
            else: self.logger.info("Successful copy {source}/{mjd}/ {destination}/{mjd}/".format(source=self.source,destination=self.destination,mjd=self.mjd))

    def touch(self, done = None, times = None):
        if self.ready:
            touch_file = "transfer-%r.done" if done else "transfer-%r.fail"
            touch_file = join(self.staging, self.log_dir, "%r" % self.mjd, touch_file % self.mjd) if self.staging and self.log_dir and self.mjd else None
            if touch_file:
                with open(touch_file, 'a'): utime(touch_file, times)
            else:
                self.logger.critical("Error touching %r" % touch_file)
            
    def drop_empty(self):
        if self.ready:
            command = "find {destination} -maxdepth 1 -type d -empty -delete".format(destination=self.destination)
            self.process.run(command)
            if self.process.status:
                self.ready = False
                self.logger.critical("Error detected while removing empty directories in {destination}.".format(destination=self.destination))

    def check_data_for_header(self,datadir,header):
        check_data_for_header = False
        default_header = {"search": None, "keyword": None, "case_insensitive": False, "contains": False, "pattern": "*", "value": None, }
        for key,value in default_header.items():
            if key not in header: header.update({key:value})
        if exists(datadir):
            search_pattern = join(datadir,header['pattern']) if header['pattern'] else datadir
            for file in iglob(search_pattern):
                if header['keyword']:
                    try: header['value'] = getval(file,header['keyword'],0)
                    except KeyError: header['value'] = None
                    if header['search'] and header['value']:
                        if header['case_insensitive']:
                            header['value'] = header['value'].lower()
                            header['search'] = header['search'].lower()
                        check_data_for_header = (header['search'] in header['value']) if header['contains'] else (header['search']==header['value'])
                        if check_data_for_header: break
        else: check_data_for_header = None
        return check_data_for_header

    def add_links(self, env_links=None):
        if self.ready and env_links:
            datadir = join(self.destination,str(self.mjd))
            sym_links = []
            for env_link in env_links:
                if '; header' in env_link:
                    env_link, header = env_link.split('; header')
                    try:
                        header = loads(header)
                        if self.check_data_for_header(datadir,header):
                            if 'search' in header:
                                self.logger.info("Checking env_link=%r: %r does contain %r data" % (env_link,datadir,header['search']))
                        else:
                            env_link = None
                            if self.check_data_for_header(datadir,header) is None:
                                self.logger.info("Cannot searching for header %r in nonexistent directory %r" % (header,datadir))
                            elif 'search' in header:
                                self.logger.info("Skipping env_link=%r: %r does not contain %r data" % (env_link,datadir,header['search']))
                    except Exception as e:
                        env_link = None
                        self.logger.info("Exception at env_link: %r " % e)

                try: sym_link = environ[env_link]
                except: sym_link=None
                if sym_link: sym_links.append(sym_link)

            if sym_links and isdir(datadir):
                for sym_link in sym_links:
                    saslink = join(sym_link,str(self.mjd))
                    if not islink(saslink) and not isdir(saslink):
                        self.logger.info("Creating symbolic link {0} -> {1}".format(datadir,saslink))
                        symlink(datadir,saslink)
                    else: self.logger.info("Skipping symbolic link: %r exists" % saslink)

    def drop_old_mjd(self, days=None):
        try: days = int(day)
        except: days = None
        if self.ready and self.source and self.mjd and days:
            old_mjd = self.mjd - days
            for mjd_dir in iglob(join(self.source,'[0-9][0-9][0-9][0-9][0-9]')):
                if isdir(mjd_dir):
                    mjd = int(basename(mjd_dir))
                    if mjd < old_mjd:
                        self.logger.info("Dropping {0} - MJD>{1} days old".format(mjd_dir,days))
                        rmtree(mjd_dir)


