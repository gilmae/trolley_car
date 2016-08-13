#!/bin/sh

find -E $TR_TORRENT_DIR -regex '(.+)[- \.][Ss]([0-9]{1,2})[eE]([0-9]{1,2}).*\.(mkv|wmv|flv|webm|mov|avi|mp4)' -exec curl -H "Content-Type: application/json" -X POST -d '{"path":"{}"}' http://localhost:3001/registerJob \;
