import datetime
from threading import Thread
import rrdtool
import sqlite3
import subprocess
import os
import csv
import smtplib
from subprocess import PIPE
from datetime import date
from fpdf import FPDF
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os.path import basename
from email.mime.application import MIMEApplication

hosts_list = []
object_list = []
emails = []
database_file = "ess_capacity.db"  # class var, change it if you want change database file name
hosts_file = "hosts.txt"  # class var, change it if you want change file name
mail_address_file = "address_mail_list.txt"  # file name with emails
database_table_name = "ess_capacity"  # class var, change it if you want change database table name
csv_report_folder_name = "csv_capacity_report"  # var folder for csv reports
txt_report_folder_name = "txt_capacity_report"  # var folder for txt reports
rrd_db_folder = "rrd_db"  # rrd db folder name
rrd_graph_folder = "rrd_graph"  # rrd graph folder name
pdf_report_folder_name = "pdf_report"  # pdf report folder name
ssh_connect_cli = "ssh -o BatchMode=yes root@"  # ssh connection cli command for subprocess
fs_names_cli = """mmlsfs all_local -T | grep "/ibm/coms" | awk '{print $2}'"""
# cli for collecting local fs data from cluster


class RrdDbWorker:

    def __init__(self, rrd_db_folder, cluster_hostname, device, fs_size, fs_current_capacity):
        self.cluster_name = cluster_hostname
        self.device = device
        self.fs_size = fs_size
        self.fs_current_capacity = fs_current_capacity
        self.rrd_db_folder = rrd_db_folder
        self.path_to_db = ""
        self.rrd_db_name = ""
        self.rrd_ds_name = ""

    def create_rrd_db_name(self):
        self.rrd_db_name = self.cluster_name + self.device + ".rrd"

    def check_if_rrd_db_in_folder(self):
        rrd_folder_db_files = os.listdir(self.rrd_db_folder)
        if self.rrd_db_name not in rrd_folder_db_files:
            return self.rrd_db_name, True
        elif self.rrd_db_name in rrd_folder_db_files:
            return self.rrd_db_name, False

    def create_rrd_db(self):
        self.cluster_name = self.cluster_name.replace("ccpr1", "")  # delete end of the hostname for rrd data store var
        self.rrd_ds_name = self.cluster_name + self.device
        self.path_to_db = self.rrd_db_folder + "/" + self.rrd_db_name
        rrdtool.create(self.path_to_db, '--step', '86400s',
                       'DS:' + self.rrd_ds_name + ':GAUGE:172800:0:' + self.fs_size,
                       'RRA:AVERAGE:0.5:1:120', "RRA:AVERAGE:0.5:7:156")

    def update_rrd_db(self):
        self.path_to_db = self.rrd_db_folder + "/" + self.rrd_db_name
        rrdtool.update(self.path_to_db, 'N:' + self.fs_current_capacity)


class RrdDbFolderClean:

    def __init__(self, object_list, rrd_db_folder):
        self.object_list = object_list
        self.rrd_db_folder = rrd_db_folder
        self.current_rrd_db = []
        self.folder_rrd_db = []

    def collect_current_rrd_db_names(self):
        for fs_object in self.object_list:
            rrd_db_path = fs_object.cluster_hostname + fs_object.device + ".rrd"
            self.current_rrd_db.append(rrd_db_path)

    def collect_current_rrd_db_from_folder(self):
        self.folder_rrd_db = os.listdir(self.rrd_db_folder)

    def delete_old_rrd_db(self):
        for rrd_db_path in self.folder_rrd_db:
            if rrd_db_path not in self.current_rrd_db:
                rrd_db_path = ''.join(rrd_db_path)
                print("RRD db will be removed " + rrd_db_path)
                os.remove(self.rrd_db_folder + "/" + rrd_db_path)
        print("RRD DB from folder :", self.folder_rrd_db)
        print("RRD DB from cluster fs list :", self.current_rrd_db)


class RrdGraphFolderClean:

    def __init__(self, object_list, rrd_graph_folder):
        self.object_list = object_list
        self.rrd_graph_folder = rrd_graph_folder
        self.current_rrd_graph = []
        self.folder_rrd_graph = []

    def collect_current_rrd_graph_names(self):
        for fs_object in self.object_list:
            rrd_graph_path = fs_object.cluster_hostname + fs_object.device + ".png"
            self.current_rrd_graph.append(rrd_graph_path)

    def collect_current_rrd_graph_from_folder(self):
        self.folder_rrd_graph = os.listdir(self.rrd_graph_folder)

    def delete_old_rrd_graph(self):
        for rrd_db_path in self.folder_rrd_graph:
            if rrd_db_path not in self.current_rrd_graph:
                rrd_db_path = ''.join(rrd_db_path)
                print("RRD Graph will be removed " + rrd_db_path)
                os.remove(self.rrd_graph_folder + "/" + rrd_db_path)
        print("RRD Graph from folder :", self.folder_rrd_graph)
        print("RRD Graph from cluster fs list :", self.current_rrd_graph)


class RrdGraphWorker:

    def __init__(self, rrd_graph_folder, cluster_hostname, device, fs_size, fs_current_capacity):
        self.cluster_name = cluster_hostname
        self.device = device
        self.fs_size = fs_size
        self.fs_current_capacity = fs_current_capacity
        self.rrd_graph_folder = rrd_graph_folder
        self.path_to_graph = ""
        self.rrd_graph_name = ""
        self.rrd_ds_name = ""
        self.rrd_db_name = ""
        self.path_to_db = ""

    def check_if_rrd_graph_in_folder(self):
        rrd_folder_graph_files = os.listdir(self.rrd_graph_folder)
        if self.rrd_graph_name not in rrd_folder_graph_files:
            return self.rrd_graph_name, True
        elif self.rrd_graph_name in rrd_folder_graph_files:
            return self.rrd_graph_name, False

    def create_rrd_graph_name(self):
        self.rrd_graph_name = self.cluster_name + self.device + ".png"

    def create_update_rrd_graph(self):
        self.rrd_db_name = self.cluster_name + self.device + ".rrd"
        self.path_to_graph = self.rrd_graph_folder + "/" + self.rrd_graph_name
        self.path_to_db = rrd_db_folder + "/" + self.rrd_db_name
        self.cluster_name = self.cluster_name.replace("ccpr1", "")  # delete end of the hostname for rrd data store var
        self.rrd_ds_name = self.cluster_name + self.device
        graph_title = self.cluster_name.replace("ccpr1", " ") + self.device
        date_time = datetime.datetime.now()
        current_size_in_pb = int(self.fs_size) * 10 ** -12
        used_size_in_pb = int(self.fs_current_capacity) * 10 ** -12
        percent_used = int(self.fs_current_capacity) / (int(self.fs_size) / 100)
        hrule = (int(self.fs_size) / 100) * 80
        rrdtool.graph(self.path_to_graph, "-w", "700", "-h", "360", "-a", "PNG", "--slope-mode",
                      "--start", "-100d", "--end", "now", "--font", "WATERMARK:7:Liberation Sans", "--font",
                      "TITLE:15:Liberation Sans", "--font", "AXIS:12:Liberation Sans", "--font",
                      "UNIT:12:Liberation Sans", "--font", "LEGEND:12:Liberation Sans", "--title",
                      graph_title + " Utilization", "--watermark", "Generated" + str(date_time), "--vertical-label",
                      "Used (TB)", "--lower-limit", "0", "--upper-limit", str(self.fs_size), "--right-axis", "1:0",
                      "--x-grid", "MONTH:1:MONTH:3:MONTH:1:0:%b", "--y-grid", str(self.fs_size) + ":2", "--rigid", "-N",
                      "--base", "1024", "DEF:" + self.rrd_ds_name + "=" + self.path_to_db + ":" + self.rrd_ds_name +
                      ":AVERAGE", "HRULE:" + str(hrule) + "#ff0000::dashes=2", "LINE2:" + self.rrd_ds_name + "#0000cc:"
                      + self.rrd_ds_name, "COMMENT: FS size PB " + str(current_size_in_pb) + " \n" + "FS used PB "
                      + str(used_size_in_pb) + " \n" + " Used in percent " + str(percent_used) + "%" + "\n")


class ServerRespond(Thread):

    def __init__(self, cluster_ip):
        Thread.__init__(self)
        self.cluster_ip = cluster_ip

    def check_server_respond(self):
        server_respond_code = subprocess.call(ssh_connect_cli + self.cluster_ip + " " + 'exit', shell=True)
        return server_respond_code, self.cluster_ip


class ServerFsDataCollect(Thread):

    def __init__(self, cluster_ip):
        Thread.__init__(self)
        self.cluster_ip = cluster_ip

    def server_available(self):
        get_cluster_fs = subprocess.Popen(ssh_connect_cli + self.cluster_ip + " " + fs_names_cli, shell=True,
                                          stdout=PIPE, stderr=PIPE)
        stdout, stderr = get_cluster_fs.communicate()
        fs_names = stdout.splitlines()
        for line in fs_names:
            fs_name = line
            get_cluster_fs_capacity = FileSystemCapacityCollector(self.cluster_ip, fs_name, ssh_connect_cli)
            get_cluster_fs_capacity.collect_server_hostname()
            get_cluster_fs_capacity.collect_cluster_fs_capacity()
            object_list.append(get_cluster_fs_capacity)

    def server_not_available(self):
        print("No connection or no filesystems collected, starting SHADOW file system collector")
        no_connection_cluster = ShadowFileSystemCollector(self.cluster_ip)
        check_ip_in_db = no_connection_cluster.check_if_data_base_entry_exists()
        if check_ip_in_db is True:
            no_connection_cluster.get_previous_day()
            file_system_data = no_connection_cluster.get_file_system_info()
            for item in file_system_data:
                fs_name, hostname, size_kb = item
                shadow_file_system_object = ShadowFileSystemInfo(fs_name, hostname, size_kb, self.cluster_ip)
                object_list.append(shadow_file_system_object)
        else:
            print("No cluster ip in db")


class FileWorker:

    def __init__(self, file_name):
        self.file_name = file_name

    def check_if_file_exists(self):
        if os.path.exists(self.file_name):
            return True
        else:
            return False

    def create_file(self):
        new_file = open(self.file_name, 'w+')
        new_file.close()
        return self.file_name

    def check_if_file_not_empty(self):
        if os.stat(self.file_name).st_size == 0:
            return False
        else:
            return True

    def read_file(self):
        file_data = []
        with open(self.file_name, "r+") as read_file:
            list_from_file = read_file.readlines()
            for item in list_from_file:
                line = ''.join(item)
                line = line.replace('\n', '')
                file_data.append(line)
        return file_data


class FolderWorker:

    def __init__(self, folder_name):
        self.folder_name = folder_name

    def check_if_folder_exists(self):
        if os.path.exists(self.folder_name):
            return True
        else:
            return False

    def create_folder(self):
        os.mkdir(self.folder_name)


class FileSystemCapacityCollector:
    fs_capacity_cli = "df -Pk"
    hostname_cli = "hostname"

    def __init__(self, cluster, fs_name, ssh_connect_cli):
        self.ssh_connect_cli = ssh_connect_cli
        self.cluster_ip = cluster
        self.cluster_hostname = ""
        self.fs_name = fs_name
        self.device = ''
        self.fs_size = 0
        self.fs_current_capacity = 0

    def collect_cluster_fs_capacity(self):
        get_fs_capacity = subprocess.Popen(self.ssh_connect_cli + self.cluster_ip + " " +
                                           FileSystemCapacityCollector.fs_capacity_cli + " " + self.fs_name,
                                           shell=True, stdout=PIPE, stderr=PIPE)
        stdout, stderr = get_fs_capacity.communicate()
        fs_out = stdout.splitlines()[1:]
        fs_out = ''.join(fs_out)
        device, size_kb, used_kb, available_kb, percent, mountpoint = fs_out.split("\n")[0].split()
        self.fs_size = size_kb
        self.fs_current_capacity = used_kb
        self.device = device

    def collect_server_hostname(self):
        get_cluster_hostname = subprocess.Popen(self.ssh_connect_cli + self.cluster_ip + " " +
                                                FileSystemCapacityCollector.hostname_cli, shell=True,
                                                stdout=PIPE, stderr=PIPE)
        stdout, stderr = get_cluster_hostname.communicate()
        hostname = str(stdout)
        hostname = hostname.replace('\n', '')
        self.cluster_hostname = hostname


class DataBaseCheck:

    def __init__(self, database_file_var):
        self.database_file = database_file_var

    def check_if_database_exists(self):
        if os.path.exists(self.database_file):
            return True
        else:
            return False

    def create_database(self):
        sqlite3.connect(self.database_file)
        return True


class DataBaseWorker:

    def __init__(self, database_file_var):
        self.database_file = database_file_var
        self.conn = sqlite3.connect(self.database_file)
        self.conn.text_factory = str
        self.cur = self.conn.cursor()
        self.date = ""
        self.database_table = database_table_name

    def check_if_table_exists(self):
        table = self.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name=?",
                             (self.database_table,))
        if table.fetchone()[0] == 1:
            print("Table exist")
            return True
        else:
            return False

    def create_table_in_data_base(self):
        return self.execute("CREATE TABLE {} (id INTEGER PRIMARY KEY AUTOINCREMENT, cluster_name text, "
                            "file_system text, size_kb int, used_kb int, date date, cluster_ip text)"
                            .format(self.database_table), ())

    def execute(self, query, data):
        self.cur.execute(query, data)
        self.conn.commit()
        return self.cur

    def today_date(self):
        date = self.execute("SELECT DATE('now', 'localtime')", ())
        date = date.fetchall()
        for item in date:
            self.date = ''.join(item)

    def check_if_date_in_db(self, cluster_ip, device):
        data = self.execute("SELECT date FROM {} WHERE date = ? AND cluster_ip = ? AND file_system = ?"
                            .format(self.database_table), (self.date, cluster_ip, device))
        data = data.fetchone()
        if data is None:
            return True
        else:
            return False

    def data_base_insert_fs_capacity(self, cluster_name, file_system, size_kb, used_kb, cluster_ip):
        return self.execute("INSERT INTO {} (cluster_name, file_system, size_kb, used_kb, date, cluster_ip) "
                            "VALUES(?,?,?,?,?,?)".format(self.database_table), (cluster_name, file_system, size_kb,
                                                                                used_kb, self.date, cluster_ip))

    def data_base_update_fs_capacity(self, cluster_name, file_system, size_kb, used_kb, cluster_ip):
        return self.execute("UPDATE {} SET size_kb = ?, used_kb = ? WHERE cluster_name = ? AND file_system = ? "
                            "AND cluster_ip = ? AND date = ?".format(self.database_table), (size_kb, used_kb,
                                                                                            cluster_name, file_system,
                                                                                            cluster_ip, self.date))


class ShadowFileSystemCollector:

    def __init__(self, cluster_ip):
        self.cluster_ip = cluster_ip
        self.database_file = database_file
        self.database_table = database_table_name
        self.file_system = ()
        self.previous_day = ""
        self.conn = sqlite3.connect(self.database_file)
        self.cur = self.conn.cursor()

    def execute(self, query, data):
        self.cur.execute(query, data)
        self.conn.commit()
        return self.cur

    def get_previous_day(self):
        date = self.execute("SELECT DATE('now', 'localtime', '-1 day')", ())
        date = date.fetchall()
        for item in date:
            self.previous_day = ''.join(item)

    def check_if_data_base_entry_exists(self):
        cluster_entry = self.execute("SELECT cluster_ip FROM {} WHERE cluster_ip = ?".format(database_table_name),
                                     (self.cluster_ip,))
        data = cluster_entry.fetchone()
        if data is None:
            return False
        else:
            return True

    def get_file_system_info(self):
        file_system_get_info = self.execute("SELECT DISTINCT file_system, cluster_name, size_kb FROM {} WHERE "
                                            "cluster_ip = ? AND date = ?".format(database_table_name),
                                            (self.cluster_ip, self.previous_day))
        self.file_system = file_system_get_info.fetchall()
        print(self.file_system)
        return self.file_system


class ShadowFileSystemInfo:

    def __init__(self, cluster_ip, fs_name, cluster_name, size_kb):
        self.cluster_ip = cluster_ip
        self.cluster_hostname = cluster_name
        self.fs_name = fs_name
        self.fs_size = size_kb
        self.fs_current_capacity = 0


class PdfCreator(FPDF):

    def lines(self):
        self.set_line_width(0.0)
        self.line(5.0, 5.0, 205.0, 5.0)  # top one
        self.line(5.0, 292.0, 205.0, 292.0)  # bottom one
        self.line(5.0, 5.0, 5.0, 292.0)  # left one
        self.line(205.0, 5.0, 205.0, 292.0)  # right one

    def add_images(self):
        rrd_graph = os.listdir(rrd_graph_folder)
        file_names = sorted(rrd_graph)
        for item in file_names:
            file_name = ''.join(item)
            file_name = rrd_graph_folder + '/' + file_name
            self.image(file_name, link='', type='png', w=180, h=80)


def hosts_file_pre_check():
    hostname_collect = FileWorker(hosts_file)
    hosts_file_check = hostname_collect.check_if_file_exists()
    if hosts_file_check is True:
        hosts_file_check_size = hostname_collect.check_if_file_not_empty()
        if hosts_file_check_size is True:
            host_names = hostname_collect.read_file()
            for item in host_names:
                hosts_list.append(item)
            print(hosts_list)
            return True
        elif hosts_file_check_size is False:
            return False
    elif hosts_file_check is False:
        hostname_collect.create_file()
        hosts_file_check = hostname_collect.check_if_file_exists()
        if hosts_file_check is False:
            return False
        elif hosts_file_check is True:
            hosts_file_check_size = hostname_collect.check_if_file_not_empty()
            if hosts_file_check_size is True:
                host_names = hostname_collect.read_file()
                for item in host_names:
                    hosts_list.append(item)
                print(hosts_list)
                return True
            elif hosts_file_check_size is False:
                return False


def data_base_check():
    database = DataBaseCheck(database_file)
    database_check = database.check_if_database_exists()
    if database_check is True:
        return True
    elif database_check is False:
        database_check = database.create_database()
        if database_check is True:
            database_check = database.check_if_database_exists()
            if database_check is True:
                return True
            elif database_check is False:
                return False


def data_base_table_check():
    database = DataBaseWorker(database_file)
    database_check = database.check_if_table_exists()
    if database_check is True:
        return True
    elif database_check is False:
        database.create_table_in_data_base()
        database_check = database.check_if_table_exists()
        if database_check is True:
            return True
        elif database_check is False:
            return False


def pre_checks_runner():
    run_hosts_file_check = hosts_file_pre_check()
    if run_hosts_file_check is True:
        print "Hosts file exists and not empty"
    elif run_hosts_file_check is False:
        print "Error! Hosts file NOT created or EMPTY, please check folder permission or create file with name {} " \
              "manually!".format(hosts_file)
    run_database_file_check = data_base_check()
    if run_database_file_check is True:
        print "Database file {} exists".format(database_file)
    elif run_hosts_file_check is False:
        print "Error! Database file NOT created, please check folder permission or create file {} manually!" \
            .format(database_file)
    run_database_table_check = data_base_table_check()
    if run_database_table_check is True:
        print "Database table {} exists".format(database_table_name)
    elif run_database_table_check is False:
        print "Error! Database table NOT created, please create table {} manually".format(database_table_name)
    if len(hosts_list) == 0:
        print "Error: Host list is empty, please check {} file and add hosts manually to the file line by line, for " \
              "example: \n 127.0.0.1 \n 127.0.0.2 \n 127.0.0.3 \n 127.0.0.4 \n ...etc...".format(hosts_file)
        return False
    elif len(hosts_list) > 1:
        print "Hosts collected from file"
        return True


def create_rrd_db_folder():
    rrd_db_path = FolderWorker(rrd_db_folder)
    rrd_db_folder_state = rrd_db_path.check_if_folder_exists()
    if rrd_db_folder_state is True:
        return True
    elif rrd_db_folder_state is False:
        rrd_db_path.create_folder()
        rrd_db_folder_state = rrd_db_path.check_if_folder_exists()
        if rrd_db_folder_state is True:
            return True
        elif rrd_db_folder_state is False:
            return False


def create_rrd_graph_folder():
    rrd_graph_path = FolderWorker(rrd_graph_folder)
    rrd_graph_folder_state = rrd_graph_path.check_if_folder_exists()
    if rrd_graph_folder_state is True:
        return True
    elif rrd_graph_folder_state is False:
        rrd_graph_path.create_folder()
        rrd_graph_folder_state = rrd_graph_path.check_if_folder_exists()
        if rrd_graph_folder_state is True:
            return True
        elif rrd_graph_folder_state is False:
            return False


def create_pdf_report_folder():
    pdf_report_folder = FolderWorker(pdf_report_folder_name)
    pdf_report_folder_state = pdf_report_folder.check_if_folder_exists()
    if pdf_report_folder_state is True:
        return True
    elif pdf_report_folder_state is False:
        pdf_report_folder.create_folder()
        rrd_graph_folder_state = pdf_report_folder.check_if_folder_exists()
        if rrd_graph_folder_state is True:
            return True
        elif rrd_graph_folder_state is False:
            return False


def create_txt_capacity_report_folder():
    txt_folder = FolderWorker(txt_report_folder_name)
    txt_folder_state = txt_folder.check_if_folder_exists()
    if txt_folder_state is True:
        return True
    elif txt_folder_state is False:
        txt_folder.create_folder()
        txt_folder_state = txt_folder.check_if_folder_exists()
        if txt_folder_state is True:
            return True
        elif txt_folder_state is False:
            return False


def create_txt_capacity_report():
    global txt_report_mail_attachment_name
    column_date = 'Date'
    column_hostname = 'Cluster'
    column_fs_name = 'FS name'
    column_fs_size = 'FS size kb'
    column_fs_used = 'FS used kb'
    proc_fs_used = 'FS used %'
    report_name = str(date.today()) + "_capacity_report.txt"
    print("creating txt report")
    txt_report_mail_attachment_name = txt_report_folder_name + "/" + report_name
    txt_report_file = open(txt_report_folder_name + "/" + report_name, "w+")
    object_list.sort(key=lambda x: x.cluster_hostname, reverse=False)
    txt_report_head = ("%-10s %-15s %-7s %-15s %-15s %s \n" % (column_date, column_hostname, column_fs_name,
                                                               column_fs_size, column_fs_used, proc_fs_used))
    txt_report_file.write(txt_report_head)
    for obj in object_list:
        proc_used = int(obj.fs_current_capacity) / (int(obj.fs_size) / 100)
        txt_report_line = ('%-10s %-15s %-7s %-15s %-15s %s  \n' % (date.today(), obj.cluster_hostname, obj.device,
                                                                    obj.fs_size, obj.fs_current_capacity,
                                                                    str(proc_used) + "%"))
        txt_report_file.write(txt_report_line)
    txt_report_file.close()
    print("capacity txt report created")


def create_csv_capacity_report_folder():
    csv_folder = FolderWorker(csv_report_folder_name)
    csv_folder_state = csv_folder.check_if_folder_exists()
    if csv_folder_state is True:
        return True
    elif csv_folder_state is False:
        csv_folder.create_folder()
        csv_folder_state = csv_folder.check_if_folder_exists()
        if csv_folder_state is True:
            return True
        elif csv_folder_state is False:
            return False


def cluster_fs_capacity_collector():
    threads = []
    for host in hosts_list:
        cluster_ip = ''.join(host)
        name = "Stream %s" % (cluster_ip + "_Checking connection")
        thread = ServerRespond(cluster_ip)
        server_response = thread.check_server_respond()
        threads.append(thread)
        thread.start()
        print(name)
        code, cluster_ip = server_response
        print(cluster_ip, code)
        fs_collect_name = "Stream %s" % (str(cluster_ip) + " " + str(code) + "_collecting FS")
        if code == 0:
            thread = ServerFsDataCollect(cluster_ip)
            thread.server_available()
            thread.start()
            print(fs_collect_name)
        elif code == 1 or 225:
            thread = ServerFsDataCollect(cluster_ip)
            thread.server_not_available()
            thread.start()
            print(fs_collect_name)


def create_csv_capacity_report():
    global csv_report_mail_attachment_name
    column_date = 'Date'
    column_hostname = 'Cluster'
    column_fs_name = 'FS name'
    column_fs_size = 'FS size kb'
    column_fs_used = 'FS used kb'
    percent_fs_used = 'FS used %'
    report_name = str(date.today()) + "_capacity_report.csv"
    csv_report_mail_attachment_name = csv_report_folder_name + "/" + report_name
    print("creating csv report")
    with open(csv_report_folder_name + "/" + report_name, "w+") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow([column_date, column_hostname, column_fs_name, column_fs_size, column_fs_used, percent_fs_used])
        for obj in object_list:
            percent_used = int(obj.fs_current_capacity) / (int(obj.fs_size) / 100)
            writer.writerow([date.today(), obj.cluster_hostname, obj.device, obj.fs_size,
                             obj.fs_current_capacity, str(percent_used) + "%"])
    print("capacity csv report created")


def check_emails_for_capacity_reports_delivery():
    global emails
    mail_file = FileWorker(mail_address_file)
    mail_file_state = mail_file.check_if_file_exists()
    if mail_file_state is True:
        check_mail_file_size = mail_file.check_if_file_not_empty()
        if check_mail_file_size is True:
            emails = mail_file.read_file()
            return True
        elif check_mail_file_size is False:
            print "Mail File is empty please add data to file {}".format(mail_address_file)
            print "example: \n email1@mail.com \n email2@mail.com \n email3@mail.com \n email4@mail.com \n ...etc..."
            print "No mail sent"
            return False
    elif mail_file_state is False:
        mail_file.create_file()
        print "File with email's address created in empty state, please add data to file {}".format(mail_address_file)
        print "example: \n email1@mail.com \n email2@2mail.com \n email3@mail.com \n email4@mail.com \n ...etc..."
        print "No mail sent"
        return False


def send_mail():
    files = [pdf_report_mail_attachment_name, csv_report_mail_attachment_name, txt_report_mail_attachment_name]
    mail_date = str(date.today().strftime('%Y-%m-%d'))
    from_mail = "mail@mail.com"
    subject = ("ESS Utilization csv, txt, pdf graph report for " + mail_date)
    mail_content = '''Hello,
    This is email with capacity reports.
    In this mail you can find attachments (CSV, TXT, PDF).
    The mail is sent using Python Automation.
    Thank You
    Developer Artem Kyrylov
    '''
    for item in emails:
        to = ''.join(item)
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = from_mail
        msg['To'] = to
        msg.attach(MIMEText(mail_content))
        for f in files:
            with open(f, "rb") as fil:
                part = MIMEApplication(fil.read(), Name=basename(f))
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
            msg.attach(part)
        server = smtplib.SMTP('localhost')
        server.sendmail(from_mail, to, msg.as_string())
        print("Report mail sent to " + to)
        server.quit()


if __name__ == "__main__":
    global pdf_report_mail_attachment_name
    pre_checks = pre_checks_runner()
    if pre_checks is True:
        cluster_fs_capacity_collector()
        for fs_object in object_list:
            database_update = DataBaseWorker(database_file)
            database_update.today_date()
            check_row_date = database_update.check_if_date_in_db(fs_object.cluster_ip, fs_object.device)
            if check_row_date is True:
                database_update.data_base_insert_fs_capacity(fs_object.cluster_hostname, fs_object.device,
                                                             fs_object.fs_size, fs_object.fs_current_capacity,
                                                             fs_object.cluster_ip)
            elif check_row_date is False:
                database_update.data_base_update_fs_capacity(fs_object.cluster_hostname, fs_object.device,
                                                             fs_object.fs_size, fs_object.fs_current_capacity,
                                                             fs_object.cluster_ip)
        txt_report_folder_check = create_txt_capacity_report_folder()
        if txt_report_folder_check is True:
            create_txt_capacity_report()
        elif txt_report_folder_check is False:
            print("No txt report folder name:{} created in auto mode, please create it or check "
                  "permissions").format(txt_report_folder_name)
        csv_report_folder_check = create_csv_capacity_report_folder()
        if csv_report_folder_check is True:
            create_csv_capacity_report()
        elif csv_report_folder_check is False:
            print("No csv report folder name:{} created in auto mode, please create it or check "
                  "permissions").format(csv_report_folder_name)
        rrd_db_folder_check = create_rrd_db_folder()
        if rrd_db_folder_check is True:
            print "RRD DB folder exist, folder name:{}".format(rrd_db_folder)
            for fs_object in object_list:
                rrd_db_worker = RrdDbWorker(rrd_db_folder, fs_object.cluster_hostname, fs_object.device,
                                            fs_object.fs_size, fs_object.fs_current_capacity)
                rrd_db_worker.create_rrd_db_name()
                check_rrd_db_file_in_folder = rrd_db_worker.check_if_rrd_db_in_folder()
                rrd_db_name, rrd_boolean = check_rrd_db_file_in_folder
                if rrd_boolean is True:
                    print "RRD DB does not exists, creating new rrd db, rrd db name:{}".format(rrd_db_name)
                    rrd_db_worker.create_rrd_db()
                    print "RRD DB will be updated, rrd db name:{}".format(rrd_db_name)
                    rrd_db_worker.update_rrd_db()
                elif rrd_boolean is False:
                    print "RRD DB exists, rrd db name:{}".format(rrd_db_name)
                    print "RDD DB will be updated name:{}".format(rrd_db_name)
                    rrd_db_worker.update_rrd_db()
            rrd_db_check = RrdDbFolderClean(object_list, rrd_db_folder)
            rrd_db_check.collect_current_rrd_db_names()
            rrd_db_check.collect_current_rrd_db_from_folder()
            rrd_db_check.delete_old_rrd_db()
        elif rrd_db_folder_check is False:
            print"RRD DB folder does not exists, folder name:{}".format(rrd_db_folder)
        rrd_graph_folder_check = create_rrd_graph_folder()
        if rrd_graph_folder_check is True:
            print "RRD GRAPH folder exist, folder name:{}".format(rrd_graph_folder)
            for fs_object in object_list:
                rrd_graph_worker = RrdGraphWorker(rrd_graph_folder, fs_object.cluster_hostname, fs_object.device,
                                                  fs_object.fs_size, fs_object.fs_current_capacity)
                rrd_graph_worker.create_rrd_graph_name()
                check_rrd_graph_file_in_folder = rrd_graph_worker.check_if_rrd_graph_in_folder()
                rrd_graph_name, rrd_boolean = check_rrd_graph_file_in_folder
                if rrd_boolean is True:
                    print "RRD GRAPH does not exists, creating new rrd graph, rrd graph name:{}".format(rrd_graph_name)
                    rrd_graph_worker.create_update_rrd_graph()
                elif rrd_boolean is False:
                    print "RRD GRAPH exists, rrd graph name:{}".format(rrd_graph_name)
                    print "RDD GRAPH will be updated name:{}".format(rrd_graph_name)
                    rrd_graph_worker.create_update_rrd_graph()
            rrd_graph_check = RrdGraphFolderClean(object_list, rrd_graph_folder)
            rrd_graph_check.collect_current_rrd_graph_names()
            rrd_graph_check.collect_current_rrd_graph_from_folder()
            rrd_graph_check.delete_old_rrd_graph()
        elif rrd_graph_folder_check is False:
            print"RRD GRAPH does not exists, folder name:{}".format(rrd_graph_folder)
        pdf_report_folder_check = create_pdf_report_folder()
        if pdf_report_folder_check is True:
            print "PDF report folder exists, folder name:{}".format(pdf_report_folder_name)
            pdf_worker = PdfCreator(orientation='P', unit='mm', format='A4')
            pdf_worker.add_page()
            pdf_worker.lines()
            pdf_worker.add_images()
            pdf_name = pdf_report_folder_name + '/' + str(date.today()) + "_capacity_report.pdf"
            pdf_report_mail_attachment_name = pdf_name
            pdf_worker.output(pdf_name, 'F')
            print "PDF report created, pdf report name:{}".format(pdf_name)
        elif pdf_report_folder_check is False:
            print "PDF report folder does not exist, folder name:{}".format(pdf_report_folder_name)
        get_mail_list_for_capacity_data_mail = check_emails_for_capacity_reports_delivery()
        mail_check = get_mail_list_for_capacity_data_mail
        if mail_check is True:
            print("All good, data added to db, mail will be sent")
            print(emails)
            send_mail()
        elif mail_check is False:
            print("Warning: Program stopped, data added to db, no mail sent, No email address found!")
    elif pre_checks is False:
        print("Error: Software dependency check's failed, please see error messages above")
