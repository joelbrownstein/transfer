from transfer import Config, Process, Logging, Summary, Backup, Copy, Globus, Rclone, Report, Sync
from os import chdir, getcwd, listdir, environ, rmdir
from os.path import join, exists, isdir, basename
import re
import gzip

class Transfer:

    drop_old_mjd_days = None

    def __init__(self, options=None, observatory=None, mjd=None, ini_mode=None, log_dir=None, include=None, exclude=None, report=False, download=False, verify=False, backup=False, copy=False, mirror=False, sync=False, debug=False, verbose=False):
        self.observatory = options.observatory if options else observatory
        self.ini_mode = options.ini_mode if options else ini_mode
        self.log_dir = options.log_dir if options else log_dir
        self.verbose = options.verbose if options else verbose
        self.mjd = options.mjd if options and options.mjd else mjd
        self.include = options.include if options else include
        self.exclude = options.exclude if options else exclude
        self.report = options.report if options else report
        self.download = options.download if options else download
        self.verify = options.verify if options else verify
        self.backup = options.backup if options else backup
        self.copy = options.copy if options else copy
        self.mirror = options.mirror if options else mirror
        self.sync = options.sync if options else sync
        self.debug = options.debug if options else debug
        self.ready = False
        self.stage = None
    
    def set_config(self):
        self.config = Config(observatory = self.observatory,  log_dir = self.log_dir, ini_mode = self.ini_mode, verbose = self.verbose)
        if not self.mjd: self.mjd = self.config.current_mjd()
        if self.verbose: print("TRANSFER> MJD=%r" % self.mjd)

    def set_logging(self):  self.logging = Logging(staging = self.config.staging, observatory = self.config.observatory, log_dir = self.config.log_dir, mode = self.config.mode, mjd = self.mjd, debug = self.debug, verbose = self.verbose)

    def set_process(self, program=None):  self.process = Process(program = program, mjd = self.mjd, logger = self.logging.logger, verbose = self.verbose)

    def set_sections(self):
        self.sections = [section for section in self.config.options.sections() if section!='general']
        if self.include: self.sections = [section for section in self.sections if section in self.include]
        if self.exclude: self.sections = [section for section in self.sections if section not in self.exclude]
        if self.verbose: print("TRANSFER> Sections=%r" % self.sections)
        self.ready = True if self.sections and self.logging.ready and self.process.ready else False
    
    def set_current_report(self):
        if self.report and self.ready:
            self.stage = 'report'
            self.logging.set_stage(stage=self.stage)
            url = self.config.options.get('general','report_url')
            report = Report(url = url, staging = self.config.staging, observatory = self.config.observatory, mjd = self.mjd, mode = self.config.mode, logger = self.logging.logger, verbose = self.verbose)
            self.current_report = basename(report.current_filename) if report.current_filename else None
        else: self.current_report = None

    def set_summary(self, mode=None, status=None):
        self.summary = Summary(staging = self.config.staging, observatory = self.config.observatory, log_dir=self.config.log_dir, mjd = self.mjd, logfile=self.current_report, verbose = self.verbose)
        if status: self.summary.todo_status = status
        for stage in self.summary.stages.keys(): self.summary.stages[stage] = getattr(self,stage)
        self.logging.logger.info("Ready to run stages [%s]" % ', '.join(self.summary.stages_todo()))
        if not self.debug: self.summary.save(stage = self.stage)


    def run_verify(self):
        if self.verify and self.ready:
            self.stage = 'verify'
            self.logging.set_stage(stage=self.stage)
            logger = self.logging.logger
            options = self.config.options
            for section in self.sections:
                boss_section = section in ['sos', 'spectro'] if section else None
                folder = join('boss',section) if boss_section else section
                mjd_dir = join(self.config.staging,folder,str(self.mjd))
                mjd_dir_nonempty = True if isdir(mjd_dir) and listdir(mjd_dir) else False
                method = options.get(section,'verify')
                if not self.debug:
                    if method != 'SKIP' and mjd_dir_nonempty:
                        sumfile = join(mjd_dir,'irsc.log.gz') if method == 'ircam' else join(mjd_dir,"{0:d}.{1}".format(self.mjd,method.split(' ')[0]))
                        if self.verbose: print("TRANSFER> Verify %s using sumfile=%r" % (section, sumfile))
                        if exists(sumfile):
                            logger.info("{0} file exists, running {1} verification stage.".format(sumfile,section))
                            if method == 'ircam':
                                cRre = re.compile(r'(cR\d{6}\.fit)(\.gz|)\s*')
                                with gzip.open(sumfile, "rt") as f: lines = f.read()
                                ircamlog = dict()
                                for l in lines.split('\n'):
                                    if len(l) > 0:
                                        m = cRre.match(l)
                                        if m is None: continue
                                        else:
                                            k = m.groups()[0]
                                            try:
                                                ircamlog[k].append(l)
                                            except KeyError:
                                                ircamlog[k] = [ l ]
                                sortedloglist = list(ircamlog.keys())
                                sortedloglist.sort()
                                sorteddisklist = list()
                                for d in listdir(mjd_dir):
                                    m = cRre.match(d)
                                    if m is not None: sorteddisklist.append(m.groups()[0])
                                sorteddisklist.sort()
                                if len(sortedloglist) == len(sorteddisklist):
                                    logger.info("Number of files in irsc.log equals number of files on disk (%r)" % len(sortedloglist))
                                    for k in range(len(sorteddisklist)):
                                        if sorteddisklist[k] != sortedloglist[k]:
                                            logger.error("WARNING: file #{0}: {1} {2}!".format(k,sorteddisklist[k],sortedloglist[k]))
                                            self.ready = False
                                else:
                                    if len(sortedloglist) > len(sorteddisklist):
                                        logger.error("Number of files in irsc.log exceeds number of files on disk (%r>%r)" % (len(sortedloglist),len(sorteddisklist)))
                                        for file in sortedloglist:
                                            if file not in sorteddisklist: logger.error("    --> Missing %s on disk" % file)
                                        for file in sorteddisklist:
                                            if file not in sortedloglist: logger.error("    --> And missing %s in irsc.log" % file)
                                    if len(sortedloglist) < len(sorteddisklist):
                                        logger.error("Fewer files in irsc.log than the number of files on disk (%r<%r)" % (len(sortedloglist),len(sorteddisklist)))
                                        for file in sorteddisklist:
                                            if file not in sortedloglist: logger.error("    --> Missing %s in irsc.log" % file)
                                        for file in sortedloglist:
                                            if file not in sorteddisklist: logger.error("    --> And Missing %s on disk" % file)
                                    self.ready = False

                            else:
                                oldwd = getcwd()
                                chdir(mjd_dir)
                                command = "{0} {1}".format(method,sumfile)
                                self.process.run(command)
                                for c in self.process.out.split("\n"):
                                    if len(c) > 0:
                                        l = c.rsplit(':',1)
                                        try: foo = l[1].index('OK')
                                        except ValueError:
                                            logger.error("Checksum mismatch: {0}".format(l[0]))
                                            self.ready = False
                                chdir(oldwd)
                        else:
                            logger.error("{0} does not appear to exist!".format(sumfile))
                            self.ready = False
                    elif not mjd_dir_nonempty: logger.info("No {0} data found.".format(section))
                if mjd_dir_nonempty:
                    self.summary.export_section(directory=mjd_dir, section=section)
                    logger.info("Export summary for section={0}.".format(section))

            if not self.debug:
                if self.ready: self.summary.save(stage=self.stage, status='success')
                else:
                    self.summary.save(stage=self.stage, status='failure')
                    logger.critical("Errors verifying {0} data!".format(section))

    def run_download(self):
        if self.download and self.ready:
            self.stage = 'download'
            self.logging.set_stage(stage=self.stage)
            logger = self.logging.logger
            options = self.config.options
            streams = options.getint('general','streams')
            perm = options.getboolean('general','permission')
            sync = Sync(staging=self.config.staging, mjd=self.mjd, streams=streams, perm=perm, process=self.process, logger=logger, verbose=self.verbose)
            if not sync.finalize:
                for sync.section in self.sections:
                    env = self.config.options.get(sync.section,'env_copy')
                    sync.set_mjd_dir(env = env)
                    sync.set_cfg(dir = self.logging.dir, stage = self.logging.stage, options = options)
                    sync.set_test()
                    if sync.test:
                        if self.verbose: print("TRANSFER> Downloading section=%r" % sync.section)
                    elif sync.test == False:
                        if self.verbose: print("TRANSFER> Skipping nonexistent section=%r" % sync.section)
                        continue
                    else:
                        self.summary.save(stage=self.stage, status='failure')
                        if self.verbose: print("TRANSFER> Critical error for section=%r" % sync.section)
                        logger.critical("Error while testing for presence of {section}/{mjd}!".format(**sync.cfg))
                        self.ready = False
                    if options.getboolean(sync.section,'multiple'): sync.run_multiple_rsync()
                    else: sync.run_single_rsync()
            if self.ready:
                self.summary.save(stage=self.stage, status='success')
            else:
                self.summary.save(stage=self.stage, status='failure')
                logger.critical("Error detected in rsync transfer of {path}".format(**sync.cfg))


    def run_copy(self):
        if self.copy and self.ready:
            self.stage = 'copy'
            self.logging.set_stage(stage=self.stage)
            resources_path = self.config.options.get('general','resources_path')
            copy = Copy(staging=self.config.staging, mjd=self.mjd, log_dir=self.config.log_dir, resources_path=resources_path, process=self.process, logger=self.logging.logger, verbose=self.verbose)
            done = None
            for section in self.sections:
                if copy.ready:
                    env = self.config.options.get(section,'env_copy')
                    partition = self.config.options.get(section,'sas_copy')
                    env_links = self.config.options.get(section,'env_link').split('\n') if self.config.options.has_option(section,'env_link') else None
                    copy.set_source(env=env, section=section)
                    copy.set_destination(env=env, partition=partition)
                    copy.copy_mjd()
                    copy.drop_empty()
                    copy.add_links(env_links=env_links)
                    copy.drop_old_mjd(days = self.drop_old_mjd_days)
                    if done is not False: done = True
                else: done = False
            copy.touch(done = done)
            if self.ready: self.summary.save(stage=self.stage, status='success')
            else: self.summary.save(stage=self.stage, status='failure')

    def run_mirror_via_backup_to_tarball(self):
        if self.mirror and self.ready:
            self.stage = 'mirror'
            self.logging.set_stage(stage=self.stage)
            logger = self.logging.logger
            backup = Backup(staging=self.config.staging, observatory=self.config.observatory, mode = self.config.mode, mjd=self.mjd, process=self.process, dir=self.logging.dir, logger=logger, stage = self.stage, verbose=self.verbose)
            if backup.ready:
                oldwd = getcwd()
                for backup.section in self.sections: backup.tar()
                chdir(oldwd)
                #backup.set_globus_transfer()
                #backup.globus_submit()
            else: self.ready = False

            if self.ready: self.summary.save(stage=self.stage, status='success')
            else:
                logger.critical("ERROR! Remote is not ready for BACKUP")
                self.summary.save(stage=self.stage, status='failure')
                
    def run_backup(self):
        if self.backup and self.ready:
            self.stage = 'backup'
            self.logging.set_stage(stage=self.stage)
            logger = self.logging.logger
            backup = Backup(staging=self.config.staging, observatory=self.config.observatory, mode = self.config.mode, mjd=self.mjd, process=self.process, dir=self.logging.dir, logger=logger, verbose=self.verbose)
            if backup.ready:
                backup.set_remote()
                if backup.remote:
                    oldwd = getcwd()
                    for backup.section in self.sections:
                        backup.tar()
                        backup.copy_to_hpss_staging()
                    chdir(oldwd)
                    """backup.set_globus_transfer()
                    backup.globus_submit()
                    backup.remote.skip_client_connect()
                    if backup.remote.connected:
                        for backup.section, backup.tarfile in backup.tarfiles.items(): backup.htar_idx()
                    backup.remote.client_close()"""
                else: self.ready = False
            else: self.ready = False

            if self.ready: self.summary.save(stage=self.stage, status='success')
            else:
                logger.critical("ERROR! Remote is not ready for BACKUP")
                self.summary.save(stage=self.stage, status='failure')

    def run_mirror(self):
        logger = self.logging.logger
        #if self.config.mode == 'mos': self.run_mirror_via_globus()
        #elif self.config.mode == 'lvm': self.run_mirror_via_sync()
        #else: logger.critical("ERROR! Invalid mode=%r for MIRROR" % self.config.mode)
        self.run_mirror_via_backup_to_tarball()
   
    def run_mirror_via_sync(self):
        if self.mirror and self.ready:
            from_sas = True
            self.stage = 'mirror'
            self.logging.set_stage(stage=self.stage)
            logger = self.logging.logger
            options = self.config.options
            streams = options.getint('general','streams')
            perm = options.getboolean('general','permission')
            sync = Sync(staging=self.config.staging, from_sas = from_sas, mjd=self.mjd, log_dir=self.config.log_dir, streams=streams, perm=perm, sync=self.sync, process=self.process, logger=logger, verbose=self.verbose)
            for sync.section in self.sections:
                env = self.config.options.get(sync.section,'env_copy')
                sync.set_mjd_dir(env = env)
                sync.set_cfg(dir = self.logging.dir, stage = self.logging.stage, options = options)
                sync.set_test()
                if not sync.test:
                    if sync.test == False: continue
                    else:
                        self.summary.save(stage=self.stage, status='failure')
                        logger.critical("Error while testing for presence of {section}/{mjd}!".format(**sync.cfg))
                        self.ready = False
                if options.getboolean(sync.section,'multiple'): sync.run_multiple_rsync()
                else: sync.run_single_rsync()
            if self.ready:
                if sync.dryrun:
                    self.summary.save(stage=self.stage, status='incomplete')
                    print(self.logging.dir)
                else:
                    sync.run_single_rsync_touch()
                    self.summary.save(stage=self.stage, status='success')
                """
                else:
                    sync.section = "lvm_spectro"
                    if sync.section in self.sections:
                        sync.set_remote()
                        if sync.remote:
                            sync.remote.client_connect()
                            sync.remote_verify()
                    self.summary.save(stage=self.stage, status='success')
                """
            else:
                self.summary.save(stage=self.stage, status='failure')
                logger.critical("Error detected in rsync transfer of {path}".format(**sync.cfg))

                
    def run_mirror_via_rclone(self):
        if self.mirror and self.ready:
            self.stage = 'mirror'
            self.logging.set_stage(stage=self.stage)
            logger = self.logging.logger
            dir = self.logging.dir
            rclone = Rclone(staging=self.config.staging, observatory=self.config.observatory, mjd=self.mjd, logger=logger, dir=dir, verbose=self.verbose)
            if rclone.ready:
                for rclone.section in self.sections:
                    rclone.env = self.config.options.get(rclone.section,'env_copy')
                    rclone.set_path()
                    rclone.append_item()
                rclone.mkdir()
                rclone.copy()
                rclone.ls()
                rclone.set_details()
                rclone.write_logfile()
            else: self.ready = False
            if self.ready: self.summary.save(stage=self.stage, status='success')
            else:
                logger.critical("ERROR! Rclone is not ready for MIRROR")
                self.summary.save(stage=self.stage, status='failure')

    def run_mirror_via_globus(self):
        if self.mirror and self.ready:
            self.stage = 'mirror'
            self.logging.set_stage(stage=self.stage)
            logger = self.logging.logger
            globus = Globus(staging=self.config.staging, observatory=self.config.observatory, mjd=self.mjd, sam=True, process=self.process, dir=self.logging.dir, logger=logger, verbose=self.verbose)
            if globus.ready:
                globus.set_options(sync = 'mtime', preserve_mtime = True, verify = True)
                for globus.section in self.sections:
                    globus.env = self.config.options.get(globus.section,'env_copy')
                    #globus.append_target_from_staging(recursive=True)
                    globus.append_target_from_env(recursive=True)
                #globus.section = 'reports'
                #globus.append_target_from_staging(resource='', recursive=True)
                #globus.section = 'atlogs'
                #globus.append_target_from_staging(recursive=True)
                #globus.append_target_from_staging(resource='index.html')
                globus.commit()
                globus.submit()
                globus.wait()
                globus.set_details()
                globus.set_status()
                globus.write_logfile()
            else: self.ready = False

            if self.ready: self.summary.save(stage=self.stage, status='success')
            else:
                logger.critical("ERROR! Globus is not ready for MIRROR")
                self.summary.save(stage=self.stage, status='failure')


    def done(self):
        self.logging.set_stage()
        self.logging.logger.info("Done!")

