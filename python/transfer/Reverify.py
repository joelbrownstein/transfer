from transfer import Config, Process, Logging, Verify
from os import chdir, getcwd, listdir, environ, rmdir
from os.path import join, exists, isdir, basename
import re
import gzip

class Reverify:

    def __init__(self, options=None, observatory=None, mjd=None, ini_mode=None, log_dir=None, include=None, exclude=None, debug=False, verbose=False):
        self.observatory = options.observatory if options else observatory
        self.ini_mode = options.ini_mode if options else ini_mode
        self.log_dir = options.log_dir if options else log_dir
        self.verbose = options.verbose if options else verbose
        self.mjd = options.mjd if options and options.mjd else mjd
        self.include = options.include if options else include
        self.exclude = options.exclude if options else exclude
        self.debug = options.debug if options else debug
        self.ready = False
        self.stage = 'reverify'
    
    def set_config(self):
        self.config = Config(observatory = self.observatory,  log_dir = self.log_dir, ini_mode = self.ini_mode, verbose = self.verbose)
        if not self.mjd: self.mjd = self.config.current_mjd()
        if self.verbose: print("REVERIFY> MJD=%r" % self.mjd)

    def set_logging(self):  self.logging = Logging(staging = self.config.staging, observatory = self.config.observatory, log_dir = self.config.log_dir, mode = self.config.mode, mjd = self.mjd, debug = self.debug, verbose = self.verbose)

    def set_process(self, program=None):  self.process = Process(program = program, mjd = self.mjd, logger = self.logging.logger, verbose = self.verbose)

    def set_sections(self):
        self.sections = [section for section in self.config.options.sections() if section!='general']
        if self.include: self.sections = [section for section in self.sections if section in self.include]
        if self.exclude: self.sections = [section for section in self.sections if section not in self.exclude]
        if self.verbose: print("REVERIFY> Sections=%r" % self.sections)
        self.ready = True if self.sections and self.logging.ready and self.process.ready else False
    
    def set_history(self, mode=None, status=None):
        self.history = History(observatory = self.config.observatory, mjd = self.mjd, mjd_dir=self.logging.mjd_log_dir, verbose = self.verbose)
        """self.summary = Summary(staging = self.config.staging, observatory = self.config.observatory, log_dir=self.config.log_dir, mjd = self.mjd, logfile=self.current_report, verbose = self.verbose)
        if status: self.summary.todo_status = status
        for stage in self.summary.stages.keys(): self.summary.stages[stage] = getattr(self,stage)
        self.logging.logger.info("Ready to run stages [%s]" % ', '.join(self.summary.stages_todo()))
        if not self.debug: self.summary.save(stage = self.stage)"""

    def run_verify(self):
        if self.ready:
            self.logging.set_stage(stage=self.stage)
            logger = self.logging.logger
            options = self.config.options
            verify = Verify(options = options, staging=self.config.staging, observatory=self.config.observatory, mode = self.config.mode, mjd=self.mjd, process=self.process, dir=self.logging.dir, logger=logger, stage = self.stage, debug = self.debug, verbose=self.verbose)
            for section in self.sections:
                verify.set_section(section = section)
                #if verify.mjd_dir_nonempty:
                #    self.summary.export_section(directory=verify.mjd_dir, section=section)
                #    logger.info("Export summary for section={0}.".format(section))
                #if not verify.ready:
                #    logger.error("{0} does not appear to exist!".format(verify.sumfile))
                #    break
            #if not self.debug:
            #    if verify.ready: self.summary.save(stage=self.stage, status='success')
            #    else:
            #        self.summary.save(stage=self.stage, status='failure')
            #        logger.critical("Errors verifying {0} data!".format(section))

    def set_summary(self, mode=None, status=None):
        self.summary = None
        
    def done(self):
        self.logging.set_stage()
        self.logging.logger.info("Done!")

class History:
    def __init__(self, observatory=None, mjd=None, mjd_dir = None, verbose=False):
        self.observatory = observatory
        self.mjd = mjd
        self.mjd_dir = mjd_dir
        self.verbose = verbose
        if self.verbose: print("HISTORY> MJD dir=%r" % self.mjd_dir)
        
