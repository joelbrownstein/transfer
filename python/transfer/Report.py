from os import stat, utime
from os.path import exists, join, basename, isdir
from glob import glob
from urllib.request import urlopen
from datetime import datetime, date, timedelta
from calendar import timegm
from re import compile, IGNORECASE
from html.parser import HTMLParser

class Report:

    def __init__(self, url=None, staging=None, observatory=None, mjd=None, mode = None, process=None, logger=None, days=None, recent_days=None, verbose=None):
        self.url = url if url != 'SKIP' else None
        self.staging = staging
        self.observatory = observatory
        self.mjd = mjd
        self.mode = mode
        self.process = process
        self.logger = logger
        self.days = days if days else 30
        self.recent_days = recent_days if recent_days else 3
        self.verbose = verbose
        self.listing = Listing()
        self.set_downloads()
        self.set_recent_downloads()
        self.set_current_filename()
    
    def set_downloads(self):
        self.downloads = []
        if self.url:
            if self.verbose: print("REPORT> set downloads for URL: %r" % self.url)
            self.logger.info("Listing: %r" % self.url)
            response = urlopen(self.url)
            headers = response.info()
            code = response.getcode()
            try: feed = response.read().decode()
            except: feed = None
            if feed:
                self.listing.feed(feed)
                response.close()
                reportdir = join(self.staging,'reports',self.mode)
                filenames = r'([0-9]{4})-([0-9]{2})-([0-9]{2})\.[0-9]{2}:[0-9]{2}:[0-9]{2}\.log'
                if self.observatory == "lco": filenames += '\.html'
                filenames = compile(filenames)
                today = date.today()
                for filename in self.listing.links:
                    if self.observatory == "lco": filename = filename.replace("%3A", ":")
                    match = filenames.match(filename)
                    if match is not None:
                        groups = match.groups()
                        year = int(groups[0])
                        month = int(groups[1])
                        day = int(groups[2])
                        download = join(reportdir,filename)
                        if (today - date(year,month,day)).days < self.days:
                            if not exists(download):
                                try:
                                    response = urlopen(join(self.url,filename))
                                    headers = response.info()
                                    code = response.getcode()
                                    path = join(reportdir,filename)
                                    with open(path,'w') as file: file.write(response.read().decode())
                                    mtime = datetime.strptime(headers['Last-Modified'],"%a, %d %b %Y %H:%M:%S %Z")
                                    st = stat(path)
                                    utime(path,(st.st_atime,timegm(mtime.timetuple())))
                                    response.close()
                                except Exception as e:
                                    if self.verbose: print("REPORT> Error in download: %r" % e)
                                    self.logger.warning("Error in download: %r" % e)
                                    download = None
                        if download: self.downloads.append(download)

    def set_recent_downloads(self):
        self.recent_downloads = []
        if self.downloads and self.recent_days:
            today = date.today()
            for day in range(self.recent_days):
                someday = (today - timedelta(day)).strftime("%Y-%m-%d")
                self.recent_downloads += [download for download in self.downloads if basename(download).startswith(someday)]
            if self.verbose: print("REPORT> %r days of recent downloads=%r" % (self.recent_days,self.recent_downloads))

    def set_current_filename(self):
        self.current_filename = None
        
        if self.recent_downloads:
            mos_variants = ["LCO Night Log \(MJD ([0-9]+)\)", "LCO Night Log  \(MJD ([0-9]+)\)", "LCO Night Log \(MJD([0-9]+)\)", "LCO Night Log \(([0-9]+)\)"]
            mos = {'apo': ['subject: 2\.5m obslog ([0-9]+) \([ms]jd ([0-9]+)\)'],
                            'lco': ['<TITLE> \[lco-operations ([0-9]+)\] %s' % variant for variant in mos_variants]}
            lvm_variants = ["LVM Observing Summary for MJD ([0-9]+)", "LVM Observing Summary MJD ([0-9]+)"]
            lvm_variants += ["\[.*\] %s" % variant for variant in lvm_variants]
            lvm = {'lco': ['<TITLE> \[lvm-inst ([0-9]+)\] %s' % variant for variant in lvm_variants]}
            subject_line = {'mos': mos, 'lvm': lvm}
            subject_mode = subject_line[self.mode] if self.mode in subject_line else []
            subject_observatories = subject_mode[self.observatory] if self.observatory in subject_mode else []
            for subject_observatory in subject_observatories:
                subject = compile(subject_observatory, IGNORECASE)
                if subject:
                    for download in self.recent_downloads:
                        self.logger.info("Open %s" % download)
                        if self.verbose: print("REPORT> OPEN %r" % download)
                        with open(download) as lines:
                            for line in lines:
                                #self.logger.warning("Match %s" % line.strip())
                                match = subject.match(line.strip())
                                if match and self.mjd == int(match.groups()[1]): self.current_filename = basename(download)
                                if self.current_filename: break
                        if self.current_filename: break
                    if self.current_filename: self.logger.info("Found %s" % self.current_filename)
                    else: self.logger.warning("None found with subject ~ %r" % subject_observatory)
        if self.verbose: print("REPORT> current_filename=%r" % self.current_filename)


class Listing(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.found_table = False
        self.found_row = False
        self.found_column = False
        self.found_link = False
        self.links = list()

    def handle_starttag(self,tag,attrs):
        if tag == "table": self.found_table = True
        elif tag == "tr": self.found_row = True
        elif tag == "td": self.found_column = True
        elif tag == "a":
            self.found_link = True
            if self.found_table and self.found_row and self.found_column:
                for a in attrs:
                    if a[0] == "href": self.links.append(basename(a[1]))
            else:
                for a in attrs:
                    if a[0] == "href":
                        if a[1].startswith("20") and a[1].endswith(".log.html"):
                            self.links.append(basename(a[1]))

    def handle_endtag(self,tag):
        if tag == "a": self.found_link = False
        elif tag == "td": self.found_column = False
        elif tag == "tr": self.found_row = False
        elif tag == "table": self.found_table = False

