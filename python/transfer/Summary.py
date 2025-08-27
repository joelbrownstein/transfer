from os import makedirs, environ, walk
from os.path import basename, exists, join
from collections import OrderedDict
from time import gmtime, strftime
from datetime import datetime
from pathlib import Path
from json import load, dump
from jinja2 import Environment, FileSystemLoader
from glob import iglob
from copy import deepcopy
from pytz import timezone


class Summary:

    stages = OrderedDict([(stage, None) for stage in ['download', 'verify', 'copy', 'mirror', 'backup']])
    colors = {"failure":"text-error", "incomplete":"text-warning","success":"text-success"}
    colors['on sas'] = colors['cita'] = colors['nersc hpss'] = colors['nersc'] = colors['unam filemon'] = "text-success"
    todo_status = "incomplete"
    mode = 0o775

    def __init__(self, staging=None, observatory=None, log_dir = None, mjd=None, logfile=None, verbose=False):
        self.staging = staging
        self.generation = 5 if 'data' in staging else 4
        self.observatory = observatory
        self.log_dir = log_dir if log_dir else 'atlogs'
        self.mjd = mjd
        self.logfile = logfile
        self.verbose = verbose
        self.set_index_template()
        self.set_indexfile()
        self.set_jsonfile()
        self.set_status()
        self.set_history()

    def export_section(self, directory = None, section = None):
        if section and directory and exists(directory):
            try:
                mjd_dir = join(self.staging,'summaries', "%r" % self.mjd)
                if not exists(mjd_dir):
                    print("SUMMARY> CREATE %r" % mjd_dir)
                    makedirs(mjd_dir, self.mode)
            except Exception as e:
                print("SUMMARY> %r" % e)
                mjd_dir = None
            if mjd_dir:
                self.navajo = timezone('Navajo')
                self.file = join(mjd_dir, "%s-%r.json" % (section, self.mjd))
                stats = [self.get_stats(directory = path, files = subfiles) for path, subdirs, subfiles in walk(directory)]
                if stats and self.file:
                    stats = [stat for substats in stats for stat in substats]
                    with open(self.file, 'w') as file: dump(stats, file, indent=4)
            else: self.file = None

    def get_stats(self, directory=None, files=None):
        stats = []
        if directory and files:
            for file in files:
                try:
                    path = join(directory, file)
                    posixpath = Path(path)
                    is_symlink = posixpath.is_symlink()
                    st = posixpath.lstat() if is_symlink else posixpath.stat()
                    mtime = self.navajo.localize(datetime.fromtimestamp(st.st_mtime)).strftime('%Y-%m-%d %H:%M:%S.%f %Z')
                    location = str(posixpath.relative_to(Path(self.staging)))
                    filename = str(posixpath.relative_to(Path(directory)))
                    stats.append({'location': location, 'filename': filename, 'path': path, 'size': st.st_size, 'mtime': mtime, 'is_symlink': is_symlink})
                except Exception as e:
                    error = "ERROR in get_stat: %r" % e
                    print(error)
        return stats

    def save(self, stage=None, status=None):
        self.append_history(stage=stage, status=status)
        self.update_jsonfile()
        self.set_indexhtml()
        self.write_indexfile()
    
    def stages_todo(self): return [stage.upper() for stage,todo in self.stages.items() if todo]

    def set_index_template(self):
        try:
            loader = FileSystemLoader(environ['TRANSFER_TEMPLATE_DIR'])
            self.index_template = Environment(loader=loader).get_template('index.html')
        except Exception as e: self.index_template = None
        if self.verbose: print("SUMMARY> index_template=%r" % self.index_template)

    def set_indexfile(self):
        self.indexfile = join(self.staging,self.log_dir,'index.html')
        if self.verbose: print("SUMMARY> indexfile=%r" % self.indexfile)

    def set_jsonfile(self):
        self.jsonfile = join(self.staging,self.log_dir,str(self.mjd),'{0:d}_status.json'.format(self.mjd))
        if self.verbose: print("SUMMARY> jsonfile=%r" % self.jsonfile)

    def set_status(self):
        if exists(self.jsonfile):
            with open(self.jsonfile) as jsonfile: self.status = load(jsonfile)
            self.current_status = deepcopy(self.status)
            if self.logfile: self.status["logfile"] = self.logfile
        else:
            self.status = {"MJD": self.mjd, "history": [], "logfile": self.logfile}
            self.current_status = None

    def append_history(self, stage=None, status=None):
        timestamp = strftime('%Y-%m-%dT%H:%M:%S',gmtime())
        if status:
            self.status["history"].append({"stage":stage,"status":status,"stamp":timestamp})
        else:
            stages = self.stages.items()
            stages_todo = [stage for stage,todo in stages if todo]
            if stages_todo:
                for stage,todo in stages: self.status["history"].append({"stage":stage,"status":self.todo_status if todo else "skip","stamp":timestamp})

    def update_jsonfile(self):
        if self.jsonfile:
            if self.status != self.current_status:
                if self.verbose: print("SUMMARY> UPDATE %r" % self.jsonfile)
                with open(self.jsonfile,'w') as jsonfile:
                    dump(self.status, jsonfile, sort_keys=True, indent=2, separators=(',',': '))
            elif self.verbose: print("SUMMARY> NO CHANGE TO %r" % self.jsonfile)

    def compressed_history(self, status):
        compressed_history = deepcopy(status)
        for stage in self.stages:
            history = [history for history in status['history'] if history['stage']==stage and history['status']!='skip']
            history = sorted(history, key=lambda x: x["stamp"], reverse=True)
            compressed_history[stage] = history[0] if history else None
        return compressed_history

    """def detailed_histories(self, histories = None):
        if histories:
            detailed_histories = []
            for history in histories:
                try: mjd = int(history['MJD'])
                except: mjd = None
                if mjd:
                    for stage in history['history']:
                        if stage['status'] == 'success':
                            if stage['stage']=='mirror': stage['status'] = 'chpc pando' if mjd > 59883 else 'nersc'
                            elif stage['stage']=='backup': stage['status'] = 'nersc hpss'
                            else: stage['status'] = 'on sas'
                    detailed_histories.append(history)
        return detailed_histories"""

    def sorted_histories(self):
        history = [self.compressed_history(self.status)] + self.history
        return sorted(history, key=lambda x: x["MJD"], reverse=True)

    def set_history(self):
        self.history = []
        for mjd_dir in iglob(join(self.staging,self.log_dir,'[0-9][0-9][0-9][0-9][0-9]')):
            jsonfile = join(mjd_dir,'{mjd}_status.json'.format(mjd=basename(mjd_dir)))
            if exists(jsonfile) and jsonfile != self.jsonfile:
                with open(jsonfile) as json: self.history.append(self.compressed_history(load(json)))



    def set_indexhtml(self):
        mode = self.log_dir.split("/")[-1] if self.log_dir else None
        title = [self.observatory.upper()] if self.observatory else []
        if mode: title.append(mode.upper())
        title = " ".join(title) if title else None
        title = title + " Data Transfer Status" if title else " Data Transfer Status"
        histories =  self.sorted_histories()
        #histories =  self.detailed_histories(self.sorted_histories())
        context = {'title': title, 'stages': self.stages, 'colors': self.colors, 'modified': datetime.utcnow(), 'histories': histories, 'observatory': self.observatory, 'mode': mode, 'generation': self.generation}
        self.indexhtml = self.index_template.render(context) if self.index_template else None

    def write_indexfile(self):
        if self.indexhtml:
            if self.verbose: print("SUMMARY> WRITE: %r" % self.indexfile)
            with open(self.indexfile,'w') as indexfile: indexfile.write(self.indexhtml)


