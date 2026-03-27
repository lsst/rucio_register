#!/bin/sh
export X509_CERT_DIR=
rucio-register zips --rucio-dataset rubin_dataset --rucio-register-config register_config.yaml --zip-file file:///rucio/disks/xrd1/rucio/test/something/2c8f9e54-9757-54c0-9119-4c3ac812a2da.zip
