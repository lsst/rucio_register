#!/bin/sh
rucio-register data-products -r /rucio/disks/xrd1/rucio/test -c HSC/runs/RC2/w_2023_32/DM-40356/20230814T170253Z -t visitSummary -d rubin_dataset -C register_config.yaml -D -s 5
rucio-register raws -r /rucio/disks/xrd1/rucio/test -d rubin_dataset -C register_config.yaml
