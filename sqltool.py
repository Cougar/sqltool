#!/usr/bin/python3

import sys
import os
import mysql.connector
import argparse

class Col(str):
    def dump_file(self, dumpfile):
        open(dumpfile, 'w').write(self.replace('\\r\\n', '\n') + '\n')


class Row(dict):
    @property
    def row_name(self):
        return self._row_name

    @row_name.setter
    def row_name(self, value):
        self._row_name = value

    def import_dir(self, configdir):
        for col in os.listdir(configdir + os.sep + self.row_name):
            self[col] = Col(open(configdir + os.sep + self.row_name + os.sep + col).read().rstrip('\n').replace('\n', '\\r\\n'))
        return self

    def dump_dir(self, dumpdir):
        os.mkdir(dumpdir + os.sep + self.row_name)
        for col in self:
            self[col].dump_file(dumpdir + os.sep + self.row_name + os.sep + col)

    def __str__(self):
        return '(' + ', '.join(['`' + key + '`' for key in self]) + ') VALUES(' + ', '.join(["'" + self[key] + "'" for key in self]) + ');'


class Table(dict):
    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    def import_dir(self, configdir):
        for row in os.listdir(configdir + os.sep + self.table_name):
            r = Row()
            r.row_name = row
            self[row] = r.import_dir(configdir + os.sep + self.table_name)
        return self

    def dump_dir(self, dumpdir):
        os.mkdir(dumpdir + os.sep + self.table_name)
        for row in self:
            self[row].dump_dir(dumpdir + os.sep + self.table_name)

    def import_sql(self, cnx):
        row_name_templ = self._row_name_template(cnx)
        cur = cnx.cursor(buffered=False)
        cur.execute('SELECT * FROM ' + self.table_name)
        fields = [desc[0] for desc in cur.description]
        for (row) in cur:
            r = Row()
            for cname, cval in zip(fields, row):
                if cval is None:
                    continue
                r[cname] = Col(str(cval).replace('\r\n', '\\r\\n'))
            try:
                row_name = row_name_templ.format(**r)
            except KeyError as ex:
                print("table_name={} row_name_templ=".format(self.table_name, row_name_templ))
                raise ex
            r.row_name = row_name
            self[row_name] = r

    def _row_name_template(self, cnx):
        cur = cnx.cursor(buffered=True)
        cur.execute('DESCRIBE ' + self.table_name)
        uni = []
        pri = []
        for (col) in cur:
            if col[3] == 'UNI':
                uni.append('{' + col[0] + '}')
            elif col[3] == 'PRI':
                pri.append('{' + col[0] + '}')
        if not uni and not pri:
            raise Exception('cant make filename without UNIQUE or PRIMARY key in %s', self.table_name)
        return '_'.join(uni) if uni else '_'.join(pri)

    def __str__(self):
        return '\n'.join(['REPLACE INTO `' + self.table_name + '` ' + str(self[row]) for row in self])


class SQL(object):
    def __init__(self, configdir='config'):
        self.tables = []

    def import_dir(self, configdir):
        for table_name in os.listdir(configdir):
            t = Table()
            t.table_name = table_name
            self.tables.append(t.import_dir(configdir))

    def import_sql(self, dbhost='localhost', dbuser='test', dbpassword='test', db='test', tables=[]):
        cnx = mysql.connector.connect(user=dbuser, password=dbpassword, host=dbhost, database=db)
        cur = cnx.cursor(buffered=True)
        cur.execute('SHOW TABLES')
        for (table_name,) in cur:
            if table_name not in tables:
                continue
            t = Table()
            t.table_name = table_name
            t.import_sql(cnx)
            self.tables.append(t)

    def dump_dir(self, dumpdir):
        os.mkdir(dumpdir)
        for table in self.tables:
            table.dump_dir(dumpdir)

    def __str__(self):
        return '\n\n'.join([str(table_name) for table_name in self.tables])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SQL import/export tool')
    fileparser = argparse.ArgumentParser(add_help=False)
    fileparser.add_argument('--from-dir', help='import directory', default=os.environ.get('FROM_DIR', None))
    sqlparser = argparse.ArgumentParser(add_help=False)
    sqlparser.add_argument('--to-dir', help='export directory', default=os.environ.get('TO_DIR', None))
    sqlparser.add_argument('--mysql-host', help='MySQL host', default=os.environ.get('MYSQL_HOST', 'localhost'))
    sqlparser.add_argument('--mysql-db', help='MySQL database', default=os.environ.get('MYSQL_DB', 'test'))
    sqlparser.add_argument('--mysql-username', help='MySQL username', default=os.environ.get('MYSQL_USERNAME', 'test'))
    sqlparser.add_argument('--mysql-password', help='MySQL password', default=os.environ.get('MYSQL_PASSWORD', None))
    sqlparser.add_argument('tables', metavar='table', type=str, nargs='+', help='table name')
    service_subparsers = parser.add_subparsers(title="command", dest="command")
    service_subparsers.add_parser("loadfiles", help="load SQL data from files", parents=[fileparser])
    service_subparsers.add_parser("dumpsql", help="dump SQL data to files", parents=[sqlparser])
    args = parser.parse_args()

    if args.command == 'loadfiles':
        if not args.from_dir:
            parser.print_help()
            sys.exit(2)
        sql = SQL()
        sql.import_dir(args.from_dir)
        print(str(sql))
    elif args.command == 'dumpsql':
        if not args.to_dir:
            parser.print_help()
            sys.exit(2)
        sql = SQL()
        sql.import_sql(args.mysql_host, args.mysql_username, args.mysql_password, args.mysql_db, args.tables)
        sql.dump_dir(args.to_dir)
    else:
        parser.print_help()
        sys.exit(2)
