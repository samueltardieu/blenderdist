#! /usr/bin/python
#
# Distribution of blender jobs
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
# targa (*.tga).
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

import cPickle, md5, os, socket, sys, time

class Communication:

    CONNECTION_CLOSED = 'CONNECTION_CLOSED'
    COMMUNICATION_ERROR = 'COMMUNICATION_ERROR'

    def __init__ (self, sock):
        """Initialize a new communication channel."""
        self.sock = sock
        self.clientfqdn = '<unknown>'

    def send_line (self, line):
        """Send a line."""
        debug ('> ' + line)
        self.sock.send (line + '\r\n')

    def get_line (self):
        """Get a line and return the words composing it in a tuple."""
        line = ''
        while True:
            c = self.sock.recv (1)
            if c == '': raise Communication.CONNECTION_CLOSED
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
            debug ('received %d bytes of %d' % (len (content), size))
            chunk = self.sock.recv (size - len (content))
            if len (chunk) == 0: raise Communication.CONNECTION_CLOSED
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
        debug ('executing the new version with args %s' % `sys.argv`)
        self.send_goodbye ()
        os.execvp ('python', ['python'] + sys.argv)

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
    the Communication class."""
    debug ('waiting for client to connect')
    newfd, client = sock.accept ()
    return Communication (newfd)

def start_server (port):
    """Start a server in IPv6 by default, IPv4 if it is not working and
    return a new socket to accept clients on."""
    debug ('starting server')
    sock = socket.socket (socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind (('', port))
    sock.listen (10)
    return sock

def start_client (host, port):
    """Start a client in IPv6 by default, IPv4 if it is not working and
    return a communication object connected to the server."""
    sock = socket.socket (socket.AF_INET, socket.SOCK_STREAM)
    sock.connect ((host, port))
    return Communication (sock)

def debug (msg):
    """Print a debug message on standard error."""
    sys.stderr.write (msg + '\n')

# My own FQDN
myfqdn = socket.getfqdn ()

# My own MD5 hex digest
mymd5 = md5_file (sys.argv[0])

CHECKSUM_MISMATCH = 'CHECKSUM_MISMATCH'
RENDERING_ERROR = 'RENDERING_ERROR'

# Global list of handled blender files, to be cleaned up at the end
blender_files = []

def client_round (host, port):
    """Make a client round, which can raise any exception. Return either
    None if there was nothing to do or (blenderfilename, blenderfilemd5,
    frametorender, resultfile) if we did a succesful rendering."""
    comm = start_client (host, port)
    try:
        debug ('sending greetings')
        comm.send_line ('HELO %s' % myfqdn)
        lastmd5, thingstodo = comm.get_line ()
        if lastmd5 != mymd5: comm.get_myself ()
        if thingstodo == '0':
            comm.send_goodbye ()
            return None
        # There is a job to be done
        comm.send_line ('REQUESTJOB')
        blenderfilename, blenderfilemd5, frametorender = comm.get_line ()
        frametorender = int (frametorender)
        fullblenderfilename = os.path.join ('/tmp', blenderfilename)
        try:
            oldmd5 = md5_file (fullblenderfilename)
            if oldmd5 != blenderfilemd5: raise CHECKSUM_MISMATCH
        except:
            # Fetch the blender file
            comm.send_line ('REQUESTBLENDERFILE %s %s' %
                            (blenderfilename, blenderfilemd5))
            size = int (comm.get_line ()[0])
            content = comm.get_content (size)
            try:
                open(fullblenderfilename, 'w').write(content)
            except:
                debug ('could not write file %s' % fullblenderfilename)
            if fullblenderfilename not in blender_files:
                blender_files.append (fullblenderfilename)
        comm.send_goodbye ()
        # Do the job
        rc = os.system ('blender -b %s -f %d' %
                        (fullblenderfilename, frametorender))
        if rc != 0: raise RENDERING_ERROR
        # Check that the output filename is present
        resultfile = os.path.join ('/tmp', '%04d.tga' % frametorender)
        try: os.stat (resultfile)
        except: raise RENDERING_ERROR
        return (blenderfilename, blenderfilemd5, frametorender, resultfile)
    finally:
        comm.shutdown ()

def client_send_result (host, port, blenderfilename, blenderfilemd5,
                        frametorender, resultfile):
    """Send the latest result without checking the client version (we
    do not want to forget to send this result."""
    comm = start_client (host, port)
    try:
        content = open(resultfile).read()
        size = len (content)
        imagename = os.path.basename (resultfile)
        comm.send_line ('RESULTS %s %s %d %s %d' %
                        (blenderfilename, blenderfilemd5, frametorender,
                         imagename, size))
        if comm.get_line()[0] == 'OK':
            comm.send_content (content)
            if comm.get_line()[0] != 'THANKYOU':
                debug ('invalid confirmation received from server')
                raise Communication.COMMUNICATION_ERROR
        comm.send_goodbye ()
    finally:
        comm.shutdown ()

def client_cleanup ():
    while blender_files:
        debug ('cleaning file %s' % blender_files[0])
        try: os.unlink (blender_files[0])
        except: pass
        del blender_files[0]

# Delay before retrying when there is nothing to do
CLIENT_WAIT_FOR_JOB = 6

# Delay before retrying when there has been an error
CLIENT_WAIT_ON_ERROR = 12

def client_main_loop (host, port):
    debug ('client %s (md5 %s) starting' % (myfqdn, mymd5))
    try:
        while True:
            # Do a job or wait until one is available
            try:
                debug ('looking for server')
                r = client_round (host, port)
                if r is None:
                    # Nothing available, do some cleanup and wait some time
                    debug ('nothing to do, waiting')
                    client_cleanup ()
                    time.sleep (CLIENT_WAIT_FOR_JOB)
                else:
                    # A job has been done
                    blenderfilename, blenderfilemd5, frametorender, \
                                     resultfile = r
                    # Loop until the result has been sent
                    while True:
                        try:
                            debug ('looking for server to send result')
                            client_send_result (host, port,
                                                blenderfilename,
                                                blenderfilemd5,
                                                frametorender,
                                                resultfile)
                            debug ('erasing resultfile %s' % resultfile)
                            try: os.unlink (resultfile)
                            except: pass
                            break
                        except:
                            debug ('server unavailable for results, waiting')
                            time.sleep (CLIENT_WAIT_ON_ERROR)
            except:
                # Error when talking with the server, wait some time
                debug ('server unavailable, waiting')
                time.sleep (CLIENT_WAIT_ON_ERROR)
    finally:
        client_cleanup ()

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
        assert (self.todo[0] == framenumber)
        del self.todo[0]
        self.distributed[framenumber] = (client, time.time())
        debug ('self.distributed = %s' % `self.distributed`)

    def needs_result (self, blendermd5, framenumber):
        debug ('%s' % `self.still_valid ()`)
        debug ('%s' % `framenumber not in self.done`)
        debug ('%s' % `self.distributed.has_key (framenumber)`)
        return self.still_valid () and \
               self.blendermd5 == blendermd5 and \
               framenumber not in self.done and \
               self.distributed.has_key (framenumber)

    def store_result (self, framenumber, imagefilename, content):
        dir = os.path.join (outputdir, self.jobname)
        try: os.stat (dir)
        except: os.mkdir (dir)
        fullimagefilename = os.path.join (dir, imagefilename)
        open(fullimagefilename, 'w').write (content)
        self.done.append (framenumber)
        del self.distributed[framenumber]

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
    for jobname, job in jobs.items ():
        if job.still_valid () == False:
            debug ('removing invalid job %s' % jobname)
            del jobs[jobname]
            continue
        n = job.next_to_do ()
        if n is not None: return job, n
    return None

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
            job, framenumber = nexttodo
            comm.send_line ('%s %s %d' % (job.blenderfilename, job.blendermd5,
                                          framenumber))
            job.assign_to_client (framenumber, comm.clientfqdn)
        elif l[0] == 'REQUESTBLENDERFILE':
            content = job.content_valid (l[1], l[2])
            debug ('len content = %d' % len (content))
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
                if job.needs_result (blenderfilemd5, framenumber) is False:
                    raise INVALID_JOB
                comm.send_line ('OK')
                content = comm.get_content (size)
                job.store_result (framenumber, imagename, content)
                comm.send_line ('THANKYOU')
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
        debug ('waiting for connection')
        comm = wait_for_client (listener)
        debug ('handling client requests')
        try:
            serve_client (comm)
            debug ('client requests honored')
        except 'foobar':
            debug ('client communication error')
        try: comm.shutdown ()
        except: pass
        save_checkpoint ()

# Main program

def main ():
    global jobdir, outputdir
    if sys.argv[1] == '--client':
        client_main_loop (sys.argv[2], int (sys.argv[3]))
    elif sys.argv[1] == '--server':
        jobdir = sys.argv[3]
        outputdir = sys.argv[4]
        server_main_loop (int (sys.argv[2]))

if __name__ == '__main__': main ()
