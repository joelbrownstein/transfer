from transfer import Config, Process, Logging, Verify
from os import chdir, getcwd, listdir, environ, rmdir
from os.path import join, exists, isdir, basename
import re
import gzip
from datetime import datetime


class Reverify:

    colors = {"failure":"text-error", "incomplete":"text-warning","success":"text-success"}

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
    
    def set_verify(self):
        if self.logging.ready:
            self.logging.set_stage(stage=self.stage)
            logger = self.logging.logger
            options = self.config.options
            self.verify = Verify(options = options, staging=self.config.staging, observatory=self.config.observatory, mode = self.config.mode, mjd=self.mjd, process=self.process, dir=self.logging.dir, logger=logger, index = self.logging.index, stage = self.stage, debug = self.debug, verbose=self.verbose)
            self.verify.set_history(mjd_log_dir = self.logging.mjd_log_dir)

        else: self.verify = None

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
    
    def run_verify(self):
        if self.ready:
            for section in self.sections:
                self.verify.set_section(section = section)
                self.verify.set_status(section = section)
                self.verify.set_history_for_section(section = section)
                self.verify.update_history(section = section)
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

    def set_mjd_history(self): self.verify.history.set_mjd_history()
        
    def save(self, mode = None):
        self.set_indexhtml(mode = mode)
        self.write_indexfile()

    def set_index_template(self):
        try:
            template_dir = join( environ['TRANSFER_TEMPLATE_DIR'], 'reverify')
            loader = FileSystemLoader(template_dir)
            self.index_template = Environment(loader=loader).get_template('index.html')
        except Exception as e: self.index_template = None
        if self.verbose: print("REVERIFY> index_template=%r" % self.index_template)

    def set_indexhtml(self, mode = None):
        title = [self.observatory.upper()] if self.observatory else []
        if mode: title.append(mode.upper())
        title = " ".join(title) if title else None
        title = title + " Data Reverify Status" if title else " Data Reverify Status"
        sections = ['apogee', 'boss', 'manga'] if self.observatory == 'apo' else ['apogee', 'boss', 'lvm']
        context = {'title': title, 'sections': sections, 'colors': self.colors, 'modified': datetime.utcnow(), 'observatory': self.observatory, 'mode': mode, 'histories': self.verify.history.mjd_history}
        self.indexhtml = self.index_template.render(context) if self.index_template else None

    def write_indexfile(self):
        if self.indexhtml:
            if self.verbose: print("REVERIFY> WRITE: %r" % self.indexfile)
            with open(self.indexfile,'w') as indexfile: indexfile.write(self.indexhtml)

    def done(self):
        self.logging.set_stage()
        self.logging.logger.info("Done!")

