#!/bin/sh
export X509_CERT_DIR=
rucio-register raws --repo /rucio/disks/xrd1/rucio/test --rucio-dataset rubin_dataset --collections LATISS/raw/all --rucio-register-config register_config.yaml
