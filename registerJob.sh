#!/bin/sh

TROLLEY_SUB_DIR=`jot -r 1 2000 130000`
mkdir "/Users/gilmae/tmp/$TROLLEY_SUB_DIR"
find -E "$TR_TORRENT_DIR" -regex '(.+)[- \.][Ss]([0-9]{1,2})[eE]([0-9]{1,2}).*\.(mkv|wmv|flv|webm|mov|avi|mp4)' -exec mv {} "/Users/gilmae/tmp/$TROLLEY_SUB_DIR/" \;
find -E "/Users/gilmae/tmp/$TROLLEY_SUB_DIR/" -regex '(.+)[- \.][Ss]([0-9]{1,2})[eE]([0-9]{1,2}).*\.(mkv|wmv|flv|webm|mov|avi|mp4)' -exec curl -H "Content-Type: application/json" -X POST -d '{"path":"{}"}' http://localhost:3001/registerJob \;
