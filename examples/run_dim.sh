#!/bin/sh
export X509_CERT_DIR=
rucio-register dimensions --rucio-dataset rubin_dataset --rucio-register-config register_config.yaml --dimension-file file:///rucio/disks/xrd1/rucio/test/something/dimensions.yaml
