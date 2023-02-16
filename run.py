# -*- coding: utf-8 -*-
"""
Created on Di 6. Jan 14:46:54 CET 2020


"""

import re
import glob
import os, sys
import errno
import numpy as np
import argparse
import fnmatch
import logging
from datetime import datetime
import pathlib
import hashlib
import json

from concurrent.futures import ThreadPoolExecutor
from elasticsearch import Elasticsearch, helpers

def recursive_glob(rootdir='.', pattern='*'):
  pdf_list = [os.path.join(looproot, filename)
    for looproot, _, filenames in os.walk(rootdir)
      for filename in filenames
        if fnmatch.fnmatch(filename, pattern)]
  reture = []
  for pdf in pdf_list:
    reture.append(os.path.dirname(pdf))
    
  return np.unique(reture)

def current_path():
    return "."

# default path is the script's current dir
def get_files_in_dir(self=current_path()):

    file_list=[]
    if os.name == 'posix':
        slash = "/" # for Linux and macOS
    else:
        slash = chr(92) # '\' for Windows

    # put a slash in dir name if needed
    if self[-1] != slash:
        self = self + slash

    # iterate the files in dir using glob
    for filename in glob.glob(self + '*.*'):
      if re.search(r''+args.match, filename):
        file_list.append(filename)

    # return the list of filenames
    return file_list

def fs_sort(files):
  files.sort(key = lambda x: x[5:-4])
  return files


def main():

    indexDirectory = raw_input('Index entire directory [Y/n]: ')
        
    if not indexDirectory:
        indexDirectory = 'y'

    if indexDirectory.lower() == 'y':
        dir = raw_input('Directory to index (relative to script): ')
        indexDir(dir)

    else:
        fname = raw_input('File to index (relative to script): ')
        createIndexIfDoesntExist()
        indexFile(fname)



def get_data_from_text_file(file):

    # declare an empty list for the data
    data = []

    # get the data line-by-line using os.open()
    for line in open(file, encoding="utf8", errors='ignore'):

        # append each line of data to the list
        data += [ str(line) ]

    # return the list of data
    return data

def prepare_merge(path, args):
    files=[]
    pattern="vendor"
    dirs = recursive_glob(args.src,'*.*')
        
    #print(dirs)
    for i in path:
        if fnmatch.fnmatch(i, pattern)==1:
            continue
        else:
            f=get_files_in_dir(i)
            if len(f) > 0:
                print("\n\n\n[{}]\t{}".format(len(f),i))
                for ff in f:
                    all_files.append(ff)
                    

    yield_docs(all_files,args.dest)            



def mymd5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


    
def current_path():
    return os.path.dirname(os.path.realpath( __file__ ))

# define a function that yields an Elasticsearch document from file data
def yield_docs(all_files,idx):

    # iterate over the list of files
    for _id, _file in enumerate(all_files):

        # use 'rfind()' to get last occurence of slash
        file_name = _file[ _file.rfind(slash)+1:]

        # get the file's statistics
        stats = os.stat( _file )
#        print(stats)
        # timestamps for the file
        create_time = datetime.fromtimestamp( stats.st_ctime )
        modify_time = datetime.fromtimestamp( stats.st_mtime )
        #size = stats.size
        
        suffix=pathlib.Path(_file).suffix
        # get the data inside the file
        data = get_data_from_text_file(_file)
        now = datetime.today()
  

        # join the list of data into one string using return
        #data = "".join( data )
        
        tags = [suffix.replace(".","")]
        
        if suffix.endswith(".json") or \
           suffix.endswith(".html") or \
           suffix.endswith(".tsv") or \
           suffix.endswith(".pdf") or \
           suffix.endswith(".csv") or \
           suffix.endswith(".tsv"):
            tags.append("text")
        else:
            tags.append("media")
        
        #if 1 in data:
        #  data = {'rows':len(data),'sample':data[0] }
        #else:
        #  data = {'rows':len(data),'sample':''}


        # create the _source data for the Elasticsearch doc
        md5s= mymd5(str(_file))
        _id = "{}_{}".format(int(stats.st_ctime),md5s)
        doc_source = {
            "file_tag": _id,
            "file_name": file_name,
            "full_path":_file,
            #"md5": md5s,
            "size":stats[6],
            #"stats":stats,
            #"type":"private",
            #"ext":suffix,
            "create_time": str(create_time),
            "modify_time": str(modify_time),
            "tags":tags,
            "info_time":str(now)
        }
        
        # use a yield generator so that the doc data isn't loaded into memory
        res = es.index(index=idx, doc_type='_doc', body=doc_source, id=_id)
        print(res)
    
    
    
if __name__ == "__main__":
    message = 0
    datetime = datetime.now()

    url = 'http://{}:{}@{}:{}'.format(os.environ.get('ELASTIC_USER'),
                                      os.environ.get('ELASTIC_PASS'),
                                      os.environ.get('ELASTIC_HOST'),
                                      os.environ.get('ELASTIC_PORT'))

    
    es = Elasticsearch([url])

    # declare a es instance of the Python Elasticsearch library
    
    # posix uses "/", and Windows uses ""
    if os.name == 'posix':
        slash = "/" # for Linux and macOS
    else:
        slash = chr(92) # '\' for Windows

    all_files = []  
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    p = argparse.ArgumentParser(prog='PROG')
    p.add_argument('--threads',   type=int, required=True)
    p.add_argument('--src',   type=str, default="~",required=False)
    p.add_argument('--dest',  type=str, default="inventar", required=False)
    p.add_argument('--match',   type=str, default="",required=False)
    p.add_argument('--ext',   type=str, default="",required=False)

    args = p.parse_args()
    dirs = recursive_glob(args.src,'*.*')

    # declare empty list for files
  
    print(args)
    # use a with statement to ensure threads are cleaned up promptly
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        future = executor.submit(prepare_merge, dirs, args)

    

