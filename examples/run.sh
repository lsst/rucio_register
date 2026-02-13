#!/bin/sh
rucio-register data-products --repo /rucio/disks/xrd1/rucio/test --collections HSC/runs/RC2/w_2023_32/DM-40356/20230814T170253Z --dataset-type visitSummary --rucio-dataset rubin_dataset --rucio-register-config  register_config.yaml --chunk-size 5 --log-level=INFO
