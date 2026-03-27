#!/bin/sh
export X509_CERT_DIR=
rucio-register data-products --log-level=INFO --repo /rucio/disks/xrd1/rucio/test --collections HSC/runs/RC2/w_2023_32/DM-40356/20230814T170253Z --dataset-type visitSummary --rucio-dataset rubin_dataset --rucio-register-config register_config.yaml --chunk-size 5 --where "instrument='HSC' and visit=322"
