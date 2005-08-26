#! /usr/bin/python
#
# Distribution of blender jobs
#
# Author: Samuel Tardieu <sam@rfc1149.net>
#
# Usage: blenderdist --server PortNumber JobDir OutputDir
#        blenderdist --client Host PortNumber
#
# A job file contains: (one on each line)
#   - the blender file name with the full path
#   - the starting frame to render
#   - the ending frame to render
#
# Frames are rendered on the clients in /tmp (which has to be set as the
# first output directory in the blender file). The output format must be
# targa (*.tga), jpeg or anything without extension (in this order). Be
# careful, if there exists a file in /tmp with a name with a higher
# priority it will wrongly be picked up.
#
# Commands from the client to the server are:
#
#     > HELO fqdnofclient
#     < lastblenderdistmd5 thingstodo
# thingstodo is 1 if there are jobs, 0 otherwise
#
#     > REQUESTBLENDERDIST
#     < size
#     < [blenderdist program]
#
#     > REQUESTJOB
#     < blenderfilename blenderfilemd5 frametorender
#  or < None
#
#     > REQUESTBLENDERFILE blenderfilename blenderfilemd5
#     < size
#     < [blenderfile]
#
#     > RESULTS blenderfilename blenderfilemd5 framenumber imagename size
#     < OK (or NOK if the job has been aborted)
#     > [blenderfilemd5]
#     < THANKYOU
#
#     > GOODBYE
#
# All exchanges use \r\n at the end of each line, \r being optional.
# MD5 digests are sent in hexadecimal format.
#
# The "python" and "blender" executables must be in the path.
#

import cPickle, md5, optparse, os, select, socket, sys, time
import thread, threading

def restart ():
    """Restart the program with the same arguments."""
    try:
        # Reap off any child process that may exist
        while True: os.wait ()
    except:
        pass
    os.execvp ('python', ['python'] + sys.argv)

# Time to wait for a new client before checking our own version or for
# client communication (to avoid being blocked)
WAIT_FOR_CLIENT_TIMEOUT = 10

class Client:

    def __init__ (self):
        self.prefetch = 10
        self.renderers = 1
        self.autoupdate = True
        parser = optparse.OptionParser ()
        parser.add_option ("--client",
                           action="store_true", dest="client",
                           help="client mode"
                           )
        parser.add_option ("--renderers", type="int", dest="renderers",
                           help="number of renderers (default: 1)")
        parser.add_option ("--prefetch", type="int", dest="prefetch",
                           help="number of prefetched jobs (default: 5)")
        parser.add_option ("--no-auto-updates", action="store_false",
                           dest="autoupdate",
                           help="disable client code auto-updates")
        (_, (host, port)) = parser.parse_args (None, self)
        self.host = host
        self.port = int (port)
        # Queues, condition variables and their associated lock
        self.jobs = []
        self.rendered = []
        self.rendering = []
        self.lock = threading.Lock ()
        self.jobs_condition = threading.Condition (self.lock)
        self.rendered_condition = threading.Condition (self.lock)
        # Last time there was no job or None if there has been work to do
        self.lastnojob = None
        # Current opened communication channel
        self.comm = None
        # Latest md5 information got from server
        self.latestmd5 = None
        # Backoff information
        self.backoff = 1
        self.blender_files = []

    def open_connection (self):
        """Open a connection to the server or raise an exception."""
        try:
            self.comm = start_client (self.host, self.port)
        except:
            debug ('error when connecting')
            raise Communication.COMMUNICATION_ERROR

    def teardown_connection (self):
        if self.comm is not None:
            debug ('disconnecting from server')
            try: self.comm.shutdown ()
            except: pass
            self.comm = None
        
    def ensure_connection (self):
        while self.comm is None:
            try:
                self.open_connection ()
                self.helo_exchange ()
            except Communication.TIMEOUT:
                self.teardown_connection ()
                debug ('server too busy')
                time.sleep (CLIENT_WAIT_ON_SERVER_BUSY)
            except Communication.COMMUNICATION_ERROR:
                self.teardown_connection ()
                debug ('server communication error')
                time.sleep (CLIENT_WAIT_ON_ERROR * self.backoff)
                self.backoff = increase_backoff (self.backoff)

    def helo_exchange (self):
        self.comm.send_line ('HELO %s' % myfqdn)
        self.latestmd5, jobstodo = self.comm.get_line ()
        if jobstodo == '0':
            debug ('no more jobs to do')
            self.lastnojob = time.time ()
            self.lock.acquire ()
            if not (self.jobs or self.rendering): self.cleanup ()
            self.lock.release ()
        else:
            self.lastnojob = None

    def send_result (self, rendered):
        fullblenderfilename, blenderfilemd5, \
                             frametorender, resultfile = \
                             rendered
        try:
            content = open(resultfile).read()
        except:
            debug ('error when reading result file %s' % resultfile)
            return
        while True:
            try:
                self.ensure_connection ()
                self.comm.send_line ('RESULTS %s %s %d %s %d' %
                                     (os.path.basename
                                     (fullblenderfilename),
                                     blenderfilemd5,
                                     frametorender,
                                     os.path.basename (resultfile),
                                     len (content)))
                if self.comm.get_line()[0] == 'OK':
                    self.comm.send_content (content)
                    if self.comm.get_line()[0] != 'THANKYOU':
                        raise Communication.COMMUNICATION_ERROR
                os.unlink (resultfile)
                break
            except Communication.TIMEOUT:
                self.teardown_connection ()
                debug ('server too busy')
                time.sleep (CLIENT_WAIT_ON_SERVER_BUSY)                    
            except Communication.COMMUNICATION_ERROR:
                self.teardown_connection ()
                debug ('server communication error')
                time.sleep (CLIENT_WAIT_ON_ERROR * self.backoff)
                self.backoff = increase_backoff (self.backoff)
                    
    def get_job (self):
        try:
            self.ensure_connection ()
            if self.latestmd5 != mymd5 and self.autoupdate:
                self.comm.get_myself ()
                self.restart ()
            if self.lastnojob is not None: return
            self.comm.send_line ('REQUESTJOB')
            l = self.comm.get_line ()
            if l == 'None': return
            blenderfilename, blenderfilemd5, frametorender = \
                             l
            frametorender = int (frametorender)
            fullblenderfilename = os.path.join ('/tmp',
                                                '%s' % blenderfilename)
            try:
                try: oldmd5 = md5_file (fullblenderfilename)
                except: raise CHECKSUM_MISMATCH
                if oldmd5 != blenderfilemd5:
                    self.lock.acquire ()
                    self.jobs = [job for job in self.jobs
                                 if job[0] != fullblenderfilename]
                    self.lock.release ()
                    raise CHECKSUM_MISMATCH
            except CHECKSUM_MISMATCH:
                self.comm.send_line ('REQUESTBLENDERFILE %s %s' %
                                     (blenderfilename, blenderfilemd5))
                size = int (self.comm.get_line ()[0])
                content = self.comm.get_content (size)
                try:
                    open(fullblenderfilename, 'w').write(content)
                except:
                    debug ('could not write file %s' % fullblenderfilename)
                    raise
                if fullblenderfilename not in self.blender_files:
                    self.blender_files.append (fullblenderfilename)
                    debug ('current blender files: %s' %
                           `self.blender_files`)
            job = fullblenderfilename, blenderfilemd5, frametorender
            self.lock.acquire ()
            # Avoid duplicating job already waiting, in progress or
            # done
            if job not in self.jobs and job not in self.rendering and \
               job not in [(j[0], j[1], j[2]) for j in self.rendered]:
                self.jobs.append (job)
                self.jobs_condition.notify ()
            else:
                debug ("not adding already existing job %s:%d" %
                       (blenderfilename, frametorender))
            self.lock.release ()
        except Communication.TIMEOUT:
            self.teardown_connection ()
            debug ('server too busy')
            time.sleep (CLIENT_WAIT_ON_SERVER_BUSY)
            raise
        except Communication.COMMUNICATION_ERROR:
            self.teardown_connection ()
            debug ('server communication error')
            time.sleep (CLIENT_WAIT_ON_ERROR * self.backoff)
            self.backoff = increase_backoff (self.backoff)
            raise

    def cleanup (self):
        while self.blender_files:
            try:
                os.unlink (self.blender_files[0])
                debug ('unlinking %s' % self.blender_files[0])
            except:
                pass
            del self.blender_files[0]

    def run (self):
        debug ('client %s (md5 %s) starting' % (myfqdn, mymd5))
        for i in range (self.renderers):
            threading.Thread (target=renderer, args=(self,)).start()
        comm, lastmd5, thingstodo = None, None, None
        try:
            # Wait until there is a result to send back or until the
            # prefetch queue is empty
            while True:
                self.backoff = 1
                while True:
                    self.lock.acquire ()
                    if not self.rendered: self.rendered_condition.wait (5)
                    if self.rendered:
                        rendered = self.rendered[0]
                        del self.rendered[0]
                        self.lock.release ()
                        self.send_result (rendered)
                    else:
                        self.lock.release ()
                        break
                while True:
                    # No need to lock for this test, we can be
                    # approximative on jobs queue size
                    if len (self.jobs) >= self.prefetch or \
                          (self.lastnojob is not None and \
                          time.time () - self.lastnojob <= \
                          CLIENT_WAIT_FOR_JOB):
                        break
                    self.lastnojob = None
                    try:
                        self.get_job ()
                    except:
                        try: self.comm.send_line ('GOODBYE')
                        except: pass
                        self.teardown_connection ()
                        break
                if self.comm is not None:
                    try: self.comm.send_line ('GOODBYE')
                    except: pass
                    self.teardown_connection ()
        finally:
            self.cleanup ()

    def run_protected (self):
        try:
            self.run ()
        except:
            self.cleanup ()
            debug (`sys.exc_info ()`)
            apply (sys.excepthook, sys.exc_info ())
            restart ()

    def restart (self):
        """Restart after all the local jobs have been terminated."""
        debug ('executing the new version')
        self.lock.acquire ()
        self.jobs = [None] * self.renderers
        self.jobs_condition.notifyAll ()
        while self.rendering:
            debug ("waiting for unfinished jobs: %s" % self.rendering)
            self.rendered_condition.wait ()
        self.lock.release ()
        restart () 

def renderer (client):
    while True:
        # Wait for a job to be available
        client.lock.acquire ()
        if not client.jobs:
            client.jobs_condition.wait ()
        job = client.jobs[0]
        del client.jobs[0]
        if job is not None: client.rendering.append (job)
        client.lock.release ()
        if job is None: thread.exit ()
        # Render it
        fullblenderfilename, blenderfilemd5, frametorender = job
        rc = os.fork ()
        try:
            if rc == -1:
                debug ('fork error')
                sys.exit (1)
            if rc == 0:
                os.execvp ('blender', ['blender', '-b', fullblenderfilename,
                                       '-f', str (frametorender)])

            else:
                (pid, status) = os.waitpid (rc, 0)
                # Check for blender errors
                if status != 0: continue
                # Check that the output filename is present
                for ext in ['.tga', '.jpg', '']:
                    resultfile = \
                               os.path.join ('/tmp', '%04d%s' % (frametorender,
                                                                 ext))
                    # Ensure that the result file exists
                    try: os.stat (resultfile)
                    except: continue
                    # Put the result in the results queue
                    client.lock.acquire ()
                    client.rendering.remove (job)
                    client.rendered.append ((fullblenderfilename,
                                             blenderfilemd5,
                                             frametorender,
                                             resultfile))
                    client.rendered_condition.notify ()
                    client.lock.release ()
                    break
                else:
                    debug ('could not find result file')
                    raise RENDERING_ERROR
        except:
            client.lock.acquire ()
            client.rendering.remove (job)
            client.lock.release ()

class Communication:

    class COMMUNICATION_ERROR (Exception): pass
    class TIMEOUT (COMMUNICATION_ERROR): pass

    def __init__ (self, sock, timeout_factor):
        """Initialize a new communication channel."""
        self.sock = sock
        self.clientfqdn = '<unknown>'
        self.timeout_factor = timeout_factor

    def send_line (self, line):
        """Send a line."""
        debug ('> ' + line)
        try: self.sock.send (line + '\r\n')
        except: raise Communication.COMMUNICATION_ERROR

    def recv (self, size):
        try:
            r, w, e = \
               select.select ([self.sock], [], [],
                              WAIT_FOR_CLIENT_TIMEOUT * self.timeout_factor)
            if self.sock in r: return self.sock.recv (size)
            raise Communication.TIMEOUT
        except:
            raise Communication.COMMUNICATION_ERROR

    def get_line (self):
        """Get a line and return the words composing it in a tuple."""
        line = ''
        while True:
            c = self.recv (1)
            if c == '': raise Communication.COMMUNICATION_ERROR
            if c == '\n': break
            if c == '\r': continue
            line += c
        debug ('< ' + line)
        return tuple (line.split())

    def send_content (self, content):
        """Send binary content over the wire, the size has been transmitted
        already."""
        debug ('> [content]')
        self.sock.send (content)

    def get_content (self, size):
        """Receive binary content."""
        debug ('will receive a file of size %d' % size)
        content = ''
        while len (content) < size:
            chunk = self.recv (size - len (content))
            if len (chunk) == 0: raise Communication.COMMUNICATION_ERROR
            content += chunk
        debug ('< [content]')
        return content

    def send_myself (self):
        """Send a copy of myself."""
        debug ('sending a copy of myself to %s' % self.clientfqdn)
        data = open(sys.argv[0]).read()
        self.send_line ("%d" % len (data))
        self.send_content (data)

    def send_goodbye (self):
        self.send_line ('GOODBYE')

    def get_myself (self):
        """Ask for a copy of the current program, replace it and reexecute
        it. This method never returns except if an exception gets raised."""
        debug ('getting a new version of myself')
        self.send_line ('REQUESTBLENDERDIST')
        size = int (self.get_line()[0])
        content = self.get_content (size)
        open(sys.argv[0], 'w').write (content)
        self.send_goodbye ()
        self.shutdown ()

    def shutdown (self):
        self.sock.shutdown (2)

def md5_content (content):
    """Return the MD5 hex digest of a string."""
    return md5.new(content).hexdigest()

def md5_file (file):
    """Return the MD5 hex digest of a file content."""
    return md5_content (open(file).read())

def wait_for_client (sock):
    """Make a server wait for a client and return a new instance of
    the Communication class or None if there has been no client after
    the timeout."""
    debug ('waiting for client to connect')
    r, w, x = select.select ([sock], [], [], WAIT_FOR_CLIENT_TIMEOUT)
    if sock in r:
        newfd, client = sock.accept ()
        return Communication (newfd, 1)
    return None

def start_server (port):
    """Start a server in IPv6 by default, IPv4 if it is not working and
    return a new socket to accept clients on."""
    debug ('starting server')
    sock = socket.socket (socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind (('', port))
    sock.listen (50)
    return sock

def start_client (host, port):
    """Start a client in IPv6 by default, IPv4 if it is not working and
    return a communication object connected to the server."""
    debug ('connecting to server')
    sock = socket.socket (socket.AF_INET, socket.SOCK_STREAM)
    sock.connect ((host, port))
    return Communication (sock, 6)

def debug (msg):
    """Print a debug message on standard error."""
    sys.stderr.write (msg + '\n')

# My own FQDN
myfqdn = socket.getfqdn ()
if myfqdn == 'localhost.localdomain': myfqdn = socket.gethostname ()

# My own MD5 hex digest (in non-interactive mode)
try: mymd5 = md5_file (sys.argv[0])
except: mymd5 = '<unknown>'

class CHECKSUM_MISMATCH (Exception): pass
class RENDERING_ERROR (Exception): pass

# Delay before retrying when there is nothing to do
CLIENT_WAIT_FOR_JOB = 180

# Delay before retrying when there has been an error
CLIENT_WAIT_ON_ERROR = 60

# Delay before retrying when there has been an error while sending results
CLIENT_WAIT_ON_RESULT_ERROR = 30

# Delay before retrying a communication when the server is too busy
CLIENT_WAIT_ON_SERVER_BUSY = 15

def increase_backoff (backoff):
    new = backoff * 1.1
    if backoff > 5: return 5
    return backoff

def blenderfilepath (filename):
    return os.path.join (jobdir, filename)

INVALID_JOB = 'INVALID_JOB'

def next_line (fd):
    line = fd.readline ()
    while line[-1:] in ['\r', '\n']: line = line[:-1]
    return line

class BlenderJob:

    def __init__ (self, jobname):        
        self.jobname = jobname
        self.fulljobname = os.path.join (jobdir, jobname)
        f = open (self.fulljobname)
        self.fullblenderfilename = next_line (f)
        self.blenderfilename = os.path.basename (self.fullblenderfilename)
        self.start = int (next_line (f))
        self.end = int (next_line (f))
        self.todo = range (self.start, self.end+1)
        self.distributed = {}
        self.done = []
        self.jobmd5 = md5_file (self.fulljobname)
        self.blendermd5 = md5_file (self.fullblenderfilename)
        self.stats = {}
        self.log ('Starting rendering')

    def log_stats (self):
        self.log ('Statistics:')
        stats = self.stats.items ()
        stats.sort (lambda x, y: cmp (y[1], x[1]))
        for host, number in stats:
            self.log ('%40s : %d frames' % (host, number))

    def still_valid (self, md5 = None):
        """Check whether this entry is still valid. If a md5 is provided,
        also check that this is the correct one for the blender file
        content."""
        try:
            if md5:
                if self.blendermd5 != md5: return False
            return self.blendermd5 == md5_file (self.fullblenderfilename) and \
                   self.jobmd5 == md5_file (self.fulljobname)
        except:
            return False

    def content_valid (self, name, md5):
        """Check that this entry is still valid and return the file
        content or raise an exception otherwise."""
        content = open(self.fullblenderfilename).read()
        if self.blendermd5 == md5_content (content) == md5 and \
           self.blenderfilename == name: return content
        debug ('invalid job (%s) or md5 (%s)' % (name, md5))
        raise INVALID_JOB

    def next_to_do (self):
        """Return the next frame to compute or None."""
        try: return self.todo[0]
        except: return None

    def assign_to_client (self, framenumber, client):
        try:
            self.todo.remove (framenumber)
            self.log ('distributing frame %d to %s' % (framenumber,
                                                       client))
        except:
            self.log ('redistributing frame %d to %s' % (framenumber,
                                                         client))
        self.distributed[framenumber] = (client, time.time())
        debug ('self.distributed = %s' % `self.distributed`)

    def already_distributed (self):
        """Return a list of (self, framenumber, time) with already
        distributed images."""
        return [(self, framenumber, date)
                for framenumber, (client, date) in self.distributed.items ()
                if framenumber not in self.done]

    def log (self, msg):
        date = time.asctime (time.localtime ())
        open (self.fulljobname[:-3] + 'log', 'a').write('%s %s\n' %
                                                        (date, msg))

    def needs_result (self, blendermd5, framenumber, client):
        if not (self.still_valid () and self.blendermd5 == blendermd5):
            self.log ('discarding frame %d from %s rendering as '
                      'file has changed' %
                      (framenumber, client))
            return False
        if framenumber in self.done or \
               not self.distributed.has_key (framenumber):
            self.log ('discarding duplicate frame %d from %s' %
                      (framenumber, client))
            return False
        return True

    def store_result (self, framenumber, imagefilename, content, client):
        self.log ('received rendered frame %d from %s' %
                  (framenumber, client))
        dir = os.path.join (outputdir, self.jobname)
        try: os.stat (dir)
        except: os.mkdir (dir)
        fullimagefilename = os.path.join (dir, imagefilename)
        open(fullimagefilename, 'w').write (content)
        self.done.append (framenumber)
        del self.distributed[framenumber]
        if self.stats.has_key (client):
            self.stats[client] += 1
        else:
            self.stats[client] = 1
        debug ('self.distributed = %s' % `self.distributed`)
        if len (self.done) == self.end - self.start + 1:
            self.log ('rendering complete')
            self.log_stats ()

def look_for_new_jobs ():
    """Look for new jobs in jobdir."""
    debug ('looking for new jobs')
    for filename in os.listdir (jobdir):
        if filename[-4:] == '.job' and not jobs.has_key (filename):
            try:
                debug ('found new job %s' % filename)
                jobs[filename] = BlenderJob (filename)
            except:
                debug ('error when adding job %s' % filename)            

def find_next_to_do ():
    """Return a (BlenderJob, framenumber) couple with the next thing
    to do or None if there is nothing to do. Also, remove invalid jobs
    if any are found."""
    look_for_new_jobs ()
    suspended = []
    for jobname, job in jobs.items ():
        if job.still_valid () == False:
            debug ('removing invalid job %s' % jobname)
            del jobs[jobname]
            continue
        # If there is a .suspend file in the same directory, disable
        # the rendering temporarily but do not loose the state
        try:
            os.stat (os.path.join (jobdir, '%s.suspend' % jobname))
            suspended.append (job)
            continue
        except:
            pass
        n = job.next_to_do ()
        if n is not None: return job, n
    # We do not have any new frame, we will retry frames in progress
    # for more than 3 minutes as some clients may have crashed
    already = []
    for job in jobs.values ():
        if job not in suspended:
            already += job.already_distributed ()
    if not already: return None
    # Sort the list so that the oldest one is first
    already.sort (lambda x, y: cmp (x[2], y[2]))
    # If the oldest is less than 3 minutes old, do not resend it
    oldest = already[0]
    if time.time() - oldest[2] < 180: return None
    return oldest[0], oldest[1]

UNKNOWN_COMMAND = 'UNKNOWN_COMMAND'

def serve_client (comm):
    """Serve a client."""
    nexttodo = None
    while True:
        l = comm.get_line ()
        if l[0] == 'HELO':
            comm.clientfqdn = l[1]
            nexttodo = find_next_to_do ()
            if nexttodo: ntd = 1
            else: ntd = 0
            comm.send_line ('%s %d' % (mymd5, ntd))
        elif l[0] == 'REQUESTBLENDERDIST':
            comm.send_myself ()
        elif l[0] == 'REQUESTJOB':
            if nexttodo is None: nexttodo = find_next_to_do ()
            if nexttodo is None:
                comm.send_line ('None')
            else:
                job, framenumber = nexttodo
                comm.send_line ('%s %s %d' %
                                (job.blenderfilename, job.blendermd5,
                                 framenumber))
                job.assign_to_client (framenumber, comm.clientfqdn)
                nexttodo = None
        elif l[0] == 'REQUESTBLENDERFILE':
            job.log ('sending blender file to %s' % comm.clientfqdn)
            content = job.content_valid (l[1], l[2])
            comm.send_line ('%d' % len (content))
            comm.send_content (content)
        elif l[0] == 'RESULTS':
            blenderfilename, blenderfilemd5, framenumber, \
                             imagename, size = l[1:]
            framenumber = int (framenumber)
            size = int (size)
            try:
                for jobname, j in jobs.items():
                    if j.blenderfilename == blenderfilename:
                        job = j
                        break
                else:
                    raise INVALID_JOB
                if job.needs_result (blenderfilemd5, framenumber,
                                     comm.clientfqdn) is False:
                    raise INVALID_JOB
                comm.send_line ('OK')
                try:
                    content = comm.get_content (size)
                    job.store_result (framenumber, imagename, content,
                                      comm.clientfqdn)
                    # The current result may have obsoleted the list
                    # of things to do
                    nexttodo = None
                    comm.send_line ('THANKYOU')
                except:
                    job.log ('communication error when receiving '
                             'rendered frame %d from %s' %
                             (framenumber, comm.clientfqdn))
                    raise
            except INVALID_JOB:
                debug ('invalid job %s (results were available)' %
                       blenderfilename)
                comm.send_line ('NOK')
        elif l[0] == 'GOODBYE':
            return
        else:
            debug ('Unknown command %s' % `l`)
            raise UNKNOWN_COMMAND

def save_checkpoint ():
    debug ('saving checkpoint')
    cPickle.dump (jobs, open (checkpointfilename, 'w'))

def load_checkpoint ():
    global jobs
    try:
        debug ('loading checkpoint')
        jobs = cPickle.load (open (checkpointfilename))
    except:
        debug ('could not read checkpoint file')
        jobs = {}

def server_main_loop (port):
    global checkpointfilename
    checkpointfilename = os.path.join (jobdir, 'blenderdist.ckp')
    load_checkpoint ()
    look_for_new_jobs ()
    listener = start_server (port)
    while True:
        comm = wait_for_client (listener)
        # If my source file has changed, reexecute myself instead of
        # sending a wrong version to the client
        if md5_file (sys.argv[0]) != mymd5:
            debug ('reloading new version of program')
            if comm is not None: comm.shutdown ()
            listener.shutdown (2)
            restart ()
        if comm is None:
            look_for_new_jobs ()
            continue
        debug ('handling client requests')
        try:
            serve_client (comm)
            debug ('client requests honored')
        except:
            debug ('client communication error')
        try: comm.shutdown ()
        except: pass
        save_checkpoint ()

# Main program

def main ():
    global jobdir, outputdir
    if '--client' in sys.argv[1:]:
        Client().run_protected()
    elif sys.argv[1] == '--server':
        jobdir = sys.argv[3]
        outputdir = sys.argv[4]
        server_main_loop (int (sys.argv[2]))

if __name__ == '__main__': main ()
