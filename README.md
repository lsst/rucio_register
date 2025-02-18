# rucio_register
Command and API to add Butler specific information to Rucio metadata.

This is a guide to using the rucio_register command for registering
Butler files with Rucio.

Butler files are expected to be located in a Rucio directory structure,
below a directory named for a Rucio scope. For example, if the root of
the Rucio directory is "/rucio/disks/xrd1/rucio" and the Rucio scope
is "test", the files should be located below "/rucio/disks/xrd1/rucio/test".


## Example

The command  "rucio-register" registers files with Rucio. This
command requires a YAML configuration file which specifies the Rucio rse and
scope, as well as the root of the directory where files are deposited,
and the external reference to the Rucio RSE. This configuration file
can be specified on the command line, or in the environment
variable **RUCIO_REGISTER_CONFIG**.

The command can register data-products or raws:

for data products:
```
rucio-register data-products -r /rucio/disks/xrd1/rucio/test -c HSC/runs/RC2/w_2023_32/DM-40356/20230814T170253Z -t visitSummary -d rubin_dataset -C register_config.yaml
```

for raws:
```
rucio-register raws -r /rucio/disks/xrd1/rucio/test -d rubin_dataset --collections LATISS/raw/all -C register_config.yaml
```
Note that for raws, this is similar to how one uses the butler command

This command looks for files registered in the butler repo "/repo/main"
using the "dataset-type" and "collections" arguments to query the butler. Note
that the repo name's suffix is the Rucio "scope". In this example, that scope
is "main".

The resulting datasets' files are registered with Rucio, as specified in
the "config.yaml" file.  Additionally, those files are registered with the
Rucio dataset specified by the "rucio-dataset" argument.


## config.yaml

The config.yaml file includes information which specifies the Rucio RSE
to use, the Rucio scope, the local root of the RSE, and the URL prefix
of the location where Rucio stores the files.


```
rucio_rse: "XRD1"
scope: "main"
rse_root: "/rucio/disks/xrd1/rucio"
dtn_url: "root://xrd1:1094//rucio"
```
