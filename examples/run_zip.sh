#!/bin/sh
#rucio-register zips -d rubin_dataset -c register_config.yaml --zip_file 2c8f9e54-9757-54c0-9119-4c3ac812a2da.zip
rucio-register zips -d rubin_dataset -C register_config.yaml --zip-file file:///rucio/disks/xrd1/rucio/test/something/2c8f9e54-9757-54c0-9119-4c3ac812a2da.zip
