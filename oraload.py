#!/usr/bin/python

# This software can be freely modified, embedded and redistributed under LGPL.
# http://www.gnu.org/copyleft/lesser.html
# Copyright Kazuki Nakajima <nkjm.kzk@gmail.com>

import optparse
import random
import string
import sys
import threading
import time
try:
    import interview
except:
    print "Failed to import interview.py. Pls check if you have installed interview.py under /usr/lib/python2.4/site-packages/. You can get interview.py from http://github.com/nkjm."
    sys.exit()
try:
    import cx_Oracle
except:
    print "Failed to import cx_Oracle.so. Pls check if you have installed cx_Oracle and also check if your LD_LIBRARY_PATH includes $ORACLE_HOME/lib."
    sys.exit()

### Configuration Area ###
sys_user = ''
sys_password = ''
default_user = ''
default_password = ''
default_ip = ''
default_service = ''

#####  Do not edit below unless you understand what you're doing  #####

commit_ratio = 10
table_name = 'oraload'
global count_now 

class Load(threading.Thread):
    def __init__(self, user, password, ip, service, op, table_name, commit_ratio, count_per_thread, id, lock):
        threading.Thread.__init__(self)
        self.i = 0
        self.user = user
        self.password = password
        self.ip = ip
        self.service = service
        self.op = op
        self.table_name = table_name
        self.commit_ratio = commit_ratio
        self.count_per_thread = count_per_thread
        self.id = id
        self.lock = lock

    def run(self):
        global count_now

        self.lock.acquire()
        try:
            conn = cx_Oracle.connect(self.user, self.password, self.ip + '/' + self.service, threaded=True)
        except cx_Oracle.DatabaseError,cx_msg:
            print "[ERROR]\tFailed to connect to Database."
            print cx_msg
            sys.exit()
        cur = conn.cursor()
        self.lock.release()

        if self.op == 'insert':
            col_title = "Your Song"
            col_artist = "Elton John"
            col_lyrics = """It's a little bit funny this feeling inside
I'm not one of those who can easily hide
I don't have much money but boy if I did
I'd buy a big house where we both could live
If I was a sculptor, but then again, no
Or a man who makes potions in a travelling show
I know it's not much but it's the best I can do
My gift is my song and this one's for you
And you can tell everybody this is your song
It may be quite simple but now that it's done
I hope you don't mind
I hope you don't mind that I put down in words
How wonderful life is while you're in the world
I sat on the roof and kicked off the moss
Well a few of the verses well they've got me quite cross
But the sun's been quite kind while I wrote this song
It's for people like you that keep it turned on
So excuse me forgetting but these things I do
You see I've forgotten if they're green or they're blue
Anyway the thing is what I really mean
Yours are the sweetest eyes I've ever seen""".replace("'", "")
            sql = "insert into %s values (record_id_seq.nextval, '%s', '%s', '%s')" % (self.table_name, col_title, col_artist, col_lyrics)
            commit_count = 0
            for i in range(self.count_per_thread):
                try:
                    cur.execute(sql)
                except cx_Oracle.DatabaseError,cx_msg:
                    print "[ERROR]\tFailed SQL: %s" % sql
                    print cx_msg
                    sys.exit()
                commit_count += 1
                if commit_count >= commit_ratio:
                    conn.commit()
                    commit_count = 0
                count_now[self.id] = i + 1
            conn.commit()

        if self.op == 'select':
            sql = 'select max(record_id) from %s' % self.table_name
            try:
                cur.execute(sql)
            except cx_Oracle.DatabaseError,cx_msg:
                print "[ERROR]\tFailed SQL: %s" % sql
                print cx_msg
                sys.exit()
            row = cur.fetchone()
            if not row[0]:
                print "There is no record."
                sys.exit()
            total_rows = int(row[0])
            for i in range(self.count_per_thread):
                needle = random.randint(1, total_rows)
                sql = 'select * from %s where record_id = %d' % (self.table_name, needle)
                try:
                    cur.execute(sql)
                except cx_Oracle.DatabaseError,cx_msg:
                    print "[ERROR]\tFailed SQL: %s" % sql
                    print cx_msg
                    sys.exit()
                rows = cur.fetchone()
                count_now[self.id] = i + 1

        cur.close()
        conn.close()

class Counter(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.i = 0

    def run(self):
        global count_now
        count_now_sum_prev = 0
        while 1 == 1:
            time.sleep(1)
            if threading.activeCount() == 2:
                break
            count_now_sum = 0
            for i in count_now:
                count_now_sum += i
            tps = count_now_sum - count_now_sum_prev
            print "TPS:\t%d" % tps
            count_now_sum_prev = count_now_sum


if __name__ == "__main__":
    usage = './oraload.py [OPTIONS]'
    version = '0.9'
    
    p = optparse.OptionParser(usage=usage, version=version)
    p.add_option("-u", "--user", action="store", type="string", dest="user", help="User Name")
    p.add_option("-p", "--password", action="store", type="string", dest="password", help="User Password")
    p.add_option("-i", "--ip", action="store", type="string", dest="ip", help="Hostname or IP Address of Oracle Database")
    p.add_option("-s", "--service", action="store", type="string", dest="service", help="Service Name of Oracle Database")
    p.add_option("-o", "--operation", action="store", type="string", dest="operation", help="Type of Load [select|insert]")
    p.add_option("-c", "--count", action="store", type="string", dest="count", help="Number of Queries to be produced")
    p.add_option("-t", "--thread", action="store", type="string", dest="thread", help="Number of Threads to be launched")
    (opts, args) = p.parse_args()
    
    intvw = interview.Interview()
    print ''

    ### Set ip
    ip = intvw.ask_new_name(question='DB IP Address', input=opts.ip, default=default_ip)

    ### Set service
    service = intvw.ask_new_name(question='DB Service Name', input=opts.service, default=default_service)

    ### Establish Connection as SYSDBA
    conn_sysdba = ''
    if (len(sys_user) != 0 and len(sys_password) != 0):
        try:
            conn_sysdba = cx_Oracle.connect(sys_user, sys_password, ip + '/' + service, cx_Oracle.SYSDBA)
        except cx_Oracle.DatabaseError,cx_msg:
            print "[ERROR]\tFailed to connect to Database as sysdba."
            print cx_msg
            sys.exit()

    ### Fetch candidate User List
    array_user_list = []
    if conn_sysdba:
        cur = conn_sysdba.cursor()
        sql = 'select username from dba_users'
        try:
            cur.execute(sql)
        except cx_Oracle.DatabaseError,cx_msg:
            print "[ERROR]\tFailed SQL: %s" % sql
            print cx_msg
            sys.exit()
        rows = cur.fetchall()
        for row in rows:
            array_user_list.append(row[0])
        array_user_list.append('+Create New User')
        cur.close()

    ### Fetch candidate Tablespace
    array_tablespace_list = []
    if conn_sysdba:
        cur = conn_sysdba.cursor()
        sql = 'select name from v$tablespace'
        try:
            cur.execute(sql)
        except cx_Oracle.DatabaseError,cx_msg:
            print "[ERROR]\tFailed SQL: %s" % sql
            print cx_msg
            sys.exit()
        rows = cur.fetchall()
        for row in rows:
            array_tablespace_list.append(row[0])
        cur.close()
    
    ### Set "user"
    if opts.user is not None:
        input_user = opts.user.upper()
    else:
        input_user = None
    if len(array_user_list) > 0:
        user = intvw.ask_name_from_list(question="USER NAME", input=input_user, default=default_user.upper(), choice_list=array_user_list)
    else:
        user = intvw.ask_new_name(question="USER NAME", input=input_user, default=default_user.upper())

    ### Create New User
    if conn_sysdba:
        if user == '+Create New User':
            user = intvw.ask_new_name(question="NEW USER NAME", input=None, default=None)
            password = intvw.ask_new_name(question="PASSWORD", input=opts.password, default=None)
            tablespace = intvw.ask_name_from_list(question='TABLESPACE', input=None, default='USERS', choice_list=array_tablespace_list)
            cur = conn_sysdba.cursor()
            sqls = []
            sqls.append('create user %s identified by %s default tablespace %s quota unlimited on %s' % (user, password, tablespace, tablespace))
            sqls.append('grant connect to %s' % user)
            sqls.append('grant resource to %s' % user)
            for sql in sqls:
                try:
                    cur.execute(sql)
                except cx_Oracle.DatabaseError,cx_msg:
                    print "[ERROR]\tFailed SQL: %s" % sql
                    print cx_msg
                    sys.exit()
            cur.close()
            print 'New User has been created.\n'
        else:
            ### Set password
            password = intvw.ask_new_name(question="PASSWORD", input=opts.password, default=default_password)
    else:
        ### Set password
        password = intvw.ask_new_name(question="PASSWORD", input=opts.password, default=default_password)

    if conn_sysdba:
        conn_sysdba.close()

    
    ### Check if table exists. If there is not, create it.
    try:
        conn = cx_Oracle.connect(user, password, ip + '/' + service)
    except cx_Oracle.DatabaseError,cx_msg:
        print "[ERROR]\tFailed to connect to Database."
        print cx_msg
        sys.exit()
    cur = conn.cursor()
    sql = "select table_name from user_tables where table_name = '%s'" % table_name.upper()
    try:
        cur.execute(sql)
    except cx_Oracle.DatabaseError,cx_msg:
        print "[ERROR]\tFailed SQL: %s" % sql
        print cx_msg
        sys.exit()
    rows = cur.fetchone()
    if not rows:
        print "Table has not been created yet. Create it right now? [y/n]: ",
        if intvw.ask_yes_or_no() == 'no':
            sys.exit()
        sql = "create table %s (record_id number, title varchar(64), artist varchar(32), lyrics varchar(2048), primary key(record_id))" % table_name
        try:
            cur.execute(sql)
        except cx_Oracle.DatabaseError,cx_msg:
            print "[ERROR]\tFailed SQL: %s" % sql
            print cx_msg
            sys.exit()
        print "\nDone.\n"
    sql = "select sequence_name from user_sequences where sequence_name = 'RECORD_ID_SEQ'"
    try:
        cur.execute(sql)
    except cx_Oracle.DatabaseError,cx_msg:
        print "[ERROR]\tFailed SQL: %s" % sql
        print cx_msg
        sys.exit()
    rows = cur.fetchone()
    if not rows:
        print "Sequence has not been created yet. Create it right now [y/n]: ",
        if intvw.ask_yes_or_no() == 'no':
            sys.exit()
        sql = "create sequence record_id_seq"
        try:
            cur.execute(sql)
        except cx_Oracle.DatabaseError,cx_msg:
            print "[ERROR]\tFailed SQL: %s" % sql
            print cx_msg
            sys.exit()
        print "\nDone.\n"
    cur.close()
    conn.close()

    ### Set "op"
    array_op_list = ('select', 'insert')
    op = intvw.ask_name_from_list(question="Type of Load", choice_list=array_op_list, input=opts.operation, default=None)

    ### Check there are records
    if op == 'select':
        try:
            conn = cx_Oracle.connect(user, password, ip + '/' + service)
        except cx_Oracle.DatabaseError,cx_msg:
            print "[ERROR]\tFailed to connect to Database."
            print cx_msg
            sys.exit()
        cur = conn.cursor()
        sql = 'select max(record_id) from %s' % table_name
        try:
            cur.execute(sql)
        except cx_Oracle.DatabaseError,cx_msg:
            print "[ERROR]\tFailed SQL: %s" % sql
            print cx_msg
            sys.exit()
        row = cur.fetchone()
        if not row[0]:
            print "There is no record.\n"
            sys.exit()
        cur.close()
        conn.close()
    
    ### Set "count"
    count = intvw.ask_number(question="Number of Queries", input=opts.count, default=None)
    
    ### Set "threads"
    threads = intvw.ask_number(question="Number of Threads", input=opts.thread, default=1)
    count_per_thread = int(round(count / threads))


    loads = []
    global count_now
    count_now = []
    count_now.append(0)
    lock = threading.Lock()

    ### Launch Threads
    for i in range(threads):
        count_now.append(0)
        loads.append(Load(user, password, ip, service, op, table_name, commit_ratio, count_per_thread, i, lock))

    time_start = time.time()

    for load in loads:
        print "#%s taking off..." % load.getName()
        load.start()
    print ''

    counter = Counter()
    counter.start()

    for load in loads:
        load.join()
    time_end = time.time()
    time_elapsed = time_end - time_start
    tps = round(count / time_elapsed)

    print ""
    print "TPS Average:\t%d" % tps
    print ""


