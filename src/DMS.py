#!/usr/bin/env python
''' DMS module is a simple api which use to store, track, and manage digital
documents on Hadoop System (HDFS, HBASE).
Author: lukkiddd
Author Site: http://www.lukkid.in.th
Github Page: https://github.com/lukkiddd/DMSHadoop
Last Modified: 25 Apr, 14:22
'''
import simplejson
from urllib2 import *

# GPL 2.0/LGPL 2.1
from starbase import Connection as hbaseConnection
# OSI Approved :: Apache Software License
from pywebhdfs.webhdfs import PyWebHdfsClient
# GNU GPL v2
from hachoir_parser import createParser
from hachoir_core.cmd_line import unicodeFilename
from hachoir_metadata import extractMetadata
from hachoir_core.i18n import getTerminalCharset
from hachoir_core.tools import makePrintable

class DMS:
    def __init__(self, debug=0):
        ''' This function use to init a class. To show an error messages debug
        should be 1.
        :param : debug - 1, show an error or success message. 0 otherwise
        :return: Nothing.
        '''
        self.debug = debug
        pass

    def hbase_connection(self, host, port, table='dms'):
        ''' This function use to establish a connection to hbase, for preparing to
        insert, remove, fetch data from hbase. We use starbase for connect to hbase
        via rest api.(See more: https://github.com/barseghyanartur/starbase)
        :param : host - hbase rest host
        :param : port - hbase rest running port
        :param : table - DMS table on hbase (default: 'dms')
        :return: Nothing.
        '''
        self.hbase = hbaseConnection(host=host, port=port)
        t = self.hbase.table(table)
        if (not t.exists()):
            t.create('meta_data','file')
        self.hbase_table = t

    def hdfs_connection(self, host, port, user_name, hdfs_path='/tmp/'):
        ''' This function use to establish a connection to hdfs, for preparing to
        create, retrieve, update, delete file in hdfs. We use pywebhdfs in order to
        do this task via hdfs rest api.(See more: http://pythonhosted.org/pywebhdfs/)
        :param : host - hdfs rest host
        :param : port - hdfs rest running port
        :param : user_name - hdfs username (for authentication)
        :param : hdfs_path - location to store files. (default: '/tmp/')
        :return: Nothing.
        '''
        self.hdfs = PyWebHdfsClient(host=host, port=port, user_name=user_name)
        self.hdfs_path = hdfs_path

    def solr_connection(self, host, port, collection):
        ''' This function use to establish a connection to solr, for query or
        search any text on a system.
        :param : host - solr's host
        :param : port - solr's running port
        :param : collection - solr's collection for searching
        '''
        self.solr = ''.join(['http://',host,':',port,'/solr/',collection])

    def extract(self, file):
        ''' This function use to extract meta data from a file. We use hachoir3 library
        to extract them. (See more: http://hachoir3.readthedocs.org)
        :param : file - file for extract
        :return: meta data as dict for success, 0 if fail.
        '''
    	try:
    		filename, realname = unicodeFilename(file), file
    		parser = createParser(filename, realname)
    		meta_data = extractMetadata(parser)
    		meta_data_text = meta_data.exportPlaintext()
    		meta_list = dict()
    		for i in range(1,len(meta_data_text)):
    			meta_split = meta_data_text[i].split(":")
    			column = meta_split[0].replace('- ','')
    			value = meta_split[1].lstrip()
    			meta_list.update({column:value})
    		return meta_list
    	except:
            if self.debug:
        		print "Something went wrong, meta data of",file,"could not extract."
            return None


    def upload(self, file):
        ''' This function use to uplaod a file to hdfs and store meta data on hbase
        Meta data consist of 2 main parts: file's meta data and hdfs's file's meta data.
        This function will increase a file version if it is already store in hbase.
        :param : file - file's name
        :return: True if success otherwise False.
        '''
        version = 1
        key = ''.join(['v',str(version),'.',file])
        path = ''.join([self.hdfs_path,key])

        # Read a file
        try:
            f = open(file,'r')
            file_content = f.read()
            f.close()
        except:
            print "Cannot read file:",file

        # Check file's version
        while self.hbase_table.fetch(key) != None:
            version = int(self.get_lastest_version(file)) + 1
            key = ''.join(['v',str(version),'.',file])
            path = ''.join([self.hdfs_path,key])

        # Try to upload file.
        try:
            self.hdfs.create_file(path,file_content)
            hdfs_meta = self.hdfs.get_file_dir_status(path)['FileStatus']
            file_meta = self.extract(file)
            t = self.hbase_table
            status = t.insert(
                key,
                {
                    'file': {'content': file_content}
                }
            )
            if status != 200:
                if self.debug:
                    print "Error inserting: file content"
            # save hbase meta data
            for i in range(0,len(file_meta.keys())):
                status = t.insert(
                    key,
                    {
                        'meta_data': {file_meta.keys()[i]: file_meta[file_meta.keys()[i]]}
                    }
                )
                if status != 200:
                    if self.debug:
                        print "Error inserting:", file_meta.keys()[i]
            # save hdfs meta data
            for i in range(0,len(hdfs_meta.keys())):
                status = t.insert(
                    key,
                    {
                        'meta_data': {hdfs_meta.keys()[i]: hdfs_meta[hdfs_meta.keys()[i]]}
                    }
                )
                if status != 200:
                    if self.debug:
                        print "Error inserting:", hdfs_meta.keys()[i]
            # save version
            status = t.insert(
                key,
                {
                    'meta_data': {'version': version}
                }
            )
            if status != 200:
                if self.debug:
                    print "Error inserting: version"
        except:
            if self.debug:
                print "Upload failed."
            return False
        if self.debug:
            print "[Uploaded]", file, "version:", version
        return True

    def download(self, file, version=None, download_dir=''):
        ''' This function use to retrieve or download file from hdfs. Then save
        it as a new file named (v[version].[file] - For example, v1.mytext.txt).
        You can specify the directory of downloaded file. You can also specify
        file's version for downloading if not it will be version 1.
        :param : file - file's name
        :param : version - file's version (default: 1)
        :param : download_dir - download directory (default: '' or current directory
                 NOTE: it must end with '/' - For example, '../download/')
        :return: True if success otherwise false.
        '''
        if not version:
            version = self.get_lastest_version(file)
        key = ''.join(['v',str(version),'.',file])
        path = ''.join([self.hdfs_path,key])
        downloaded_file = ''.join([download_dir,key])
        try:
            f = open(downloaded_file, 'w')
            f.write(self.hdfs.read_file(path))
            f.close()
        except:
            if self.debug:
                print "Cannot download a file:", file
            return False
        if self.debug:
            print "[Downloaded]",key
        return True

    def update(self, file, version=None):
        ''' This function use to update file to hdfs and data stored in hbase by
        overwrite that file on hdfs, and also insert new data to hbase too. You can
        specify a file's version in order to update it.
        :param : file - file's name
        :param : version - file's version
        :return: True if success otherwise False.
        '''
        if not version:
            version = self.get_lastest_version(file)
        key = ''.join(['v',str(version),'.',file])
        path = ''.join([self.hdfs_path,key])

        # Read a file
        try:
            f = open(file,'r')
            file_content = f.read()
            f.close()
        except:
            print "Cannot read file:",file

        # Try to upload file.
        try:
            self.hdfs.create_file(path,file,overwrite=True)
            hdfs_meta = self.hdfs.get_file_dir_status(path)['FileStatus']
            file_meta = self.extract(file)
            status = t.insert(
                key,
                {
                    'file': {'content': file_content,
                             'name': file}
                }
            )
            if status != 200:
                if self.debug:
                    print "Error inserting: file content"

            # save hbase meta data
            for i in range(0,len(file_meta.keys())):
                status = t.insert(
                    key,
                    {
                        'meta_data': {file_meta.keys()[i]: file_meta[file_meta.keys()[i]]}
                    }
                )
                if status != 200:
                    if self.debug:
                        print "Error inserting:", file_meta.keys()[i]
            # save hdfs meta data
            for i in range(0,len(hdfs_meta.keys())):
                status = t.insert(
                    key,
                    {
                        'meta_data': {hdfs_meta.keys()[i]: hdfs_meta[hdfs_meta.keys()[i]]}
                    }
                )
                if status != 200:
                    if self.debug:
                        print "Error inserting:", hdfs_meta.keys()[i]
            # save version
            status = t.insert(
                key,
                {
                    'meta_data': {'version': version}
                }
            )
            if status != 200:
                if self.debug:
                    print "Error inserting: version"
        except:
            if self.debug:
                print "Update failed."
            return False
        if self.debug:
            print "[Updated]", file, "version:", version
        return True

    def delete(self, file, version=None):
        ''' This function use to delete file in hbase, and hdfs. You can specify
        file's version in order to delete it.
        :param : file - file's name
        :param : version - file's version
        :return: True if succes otherwise False.
        '''
        if not version:
            version = self.get_lastest_version(file)
        key = ''.join(['v',str(version),'.',file])
        path = ''.join([self.hdfs_path,key])

        # Check if file exists
        if self.hbase_table.fetch(key) == None:
            if self.debug:
                print "Cannot delete.",key,"is not exists."
            return False

        # Remove row on hbase
        t = self.hbase_table
        if t.remove(key) != 200:
            if self.debug:
                print "[HBASE] cannot remove a row key:",key
            return False

        # Delete file on hdfs
        if not self.hdfs.delete_file_dir(path):
            if self.debug:
                print "[HDFS] Cannot remove a file path:",path
            return False
        if self.debug:
            print "[Deleted]", file, "version:", version
        return True

    def get_file_meta_data(self, file, version=None):
        ''' This function use to get all file's meta_data from hbase. You can
        specify a file's version.
        :param : file - file's name
        :param : version - file's version
        :return: meta data as dict for success, 0 if fail
        '''
        if not version:
            version = self.get_lastest_version(file)
        key = ''.join(['v',str(version),'.',file])
        if not self.hbase_table.fetch(key):
            if self.debug:
                print key,"is not exists"
            return False
        return self.hbase_table.fetch(key)['meta_data']

    def get_file_content(self, file, version=None):
        ''' This function use to get all file's content from hbase. You can
        specify a file's version.
        :param : file - file's name
        :param : version - file's version
        :return: meta data as dict for success, 0 if fail
        '''
        if not version:
            version = self.get_lastest_version(file)
        key = ''.join(['v',str(version),'.',file])
        if not self.hbase_table.fetch(key):
            if self.debug:
                print key,"is not exists"
            return False
        return self.hbase_table.fetch(key)['file']

    def search(self, text):
        ''' This function will search in xxxx via solr rest api.
        :param : text - text for searching
        :return: json response from solr, False for not found.
        '''
        query = urlopen(''.join([self.solr,'/select?q=',text,'&wt=json']))
        response = simplejson.load(query)
        if response['response']['numFound'] == 0:
            if self.debug:
                print text,"not found!"
            return False
        return response

    def get_all_file(self):
        ''' This function return all files that stored on Hbase in a list format.
        :param : Nothing.
        :return: fetch result as a list.
        '''
        rf = '{"type": "RowFilter", "op": "EQUAL", "comparator": {"type": "RegexStringComparator", "value": ""}}'
        t = self.hbase_table
        result = t.fetch_all_rows(with_row_id=True, filter_string=rf)
        return list(result)

    def get_file_version(self, file):
        ''' This function will fetch data from file name then return them.
        :param : file - file's name
        :return: file_list with version as a dict.
        '''
        rf = ''.join(['{"type": "RowFilter", "op": "EQUAL", "comparator": {"type": "RegexStringComparator", "value": "',file,'"}}'])
        t = self.hbase_table
        result = t.fetch_all_rows(with_row_id=True, filter_string=rf)
        lsr = list(result)
        file_version = list()
        for i in range(0,len(lsr)):
            file_version.append(lsr[i].keys()[0].split('.')[0].split('v')[1])
        file_list = dict()
        file_list['name'] = file
        file_list['version'] = file_version
        return file_list

    def get_lastest_version(self, file):
        ''' This function will return a lastest version number as integer.
        :param : file - file's name
        :return: version number as an integer.
        '''
        file_version = self.get_file_version(file)
        file_version['version'].sort()
        return file_version['version'][len(file_version['version'])-1]

    def delete_all_version(self, file):
        ''' This function will delete all file's version in an hbase and HDFS
        :param : file - file's name
        :return: True if success otherwise False
        '''
        self.get_file_version(file)['version'].sort()
        for version in self.get_file_version(file)['version']:
            try:
                self.delete(file,version)
            except:
                return False
        return True

    def delete_all(self):
        ''' This function will delete all the files on an hbase and hdfs.
        :param : Nothing
        :return: True if success otherwise False
        '''
        for full_file in self.get_all_file():
            file = full_file.keys()[0].split('.')[1]
            version = full_file.keys()[0].split('.')[0].split('v')[1]
            try:
                self.delete(file,version)
            except:
                return False
        return True
