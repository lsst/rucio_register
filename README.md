# rucio-register
Command and API to add Butler specific information to Rucio metadata.

This is a guide to using the rucio-register command for registering
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
rucio-register data-products --log-level INFO --repo /rucio/disks/xrd1/rucio/test --collections HSC/runs/RC2/w_2023_32/DM-40356/20230814T170253Z --dataset-type  visitSummary --rucio-dataset rubin_dataset --rucio-register-config register_config.yaml
```

for raws:
```
rucio-register raws --log-level INFO --repo /rucio/disks/xrd1/rucio/test --rucio-dataset rubin_dataset --collections LATISS/raw/all --rucio-register-config register_config.yaml \*
```
Note that for raws, this is similar to how one uses the butler command

This command looks for files registered in the butler repo "/repo/main"
using the "dataset-type" and "collections" arguments to query the butler. Note
that the repo name's suffix is the Rucio "scope". In this example, that scope
is "main".

The resulting datasets' files are registered with Rucio, as specified in
the "config.yaml" file.  Additionally, those files are registered with the
Rucio dataset specified by the "rucio-dataset" argument.

for zip files:
```
rucio-register zips --rucio-dataset rubin_dataset --log-level INFO --rucio-register-config /home/lsst/rucio_register/examples/register_config.yaml --zip-file file:///rucio/disks/xrd1/rucio/test/something/2c8f9e54-9757-54c0-9119-4c3ac812a2da.zip
```
Note for zip files, register a single zip file at a time.

for dimension record YAML files:
```
rucio-register dimensions --rucio-dataset rubin_dataset --log-level INFO --rucio-register-config /home/lsst/rucio_register/examples/register_config.yaml --dimension-file file:///rucio/disks/xrd1/rucio/test/something/dimensions.yaml
```
Note for zip files, register a single zip file at a time.


# Retries in case of failure

There are a variety of reasons that registeration to Rucio can fail.  In an effort to prevent a failure from completely halting the command, rucio-register will attempt to retry registration,
and uses exponential backoff as a retry strategy where the wait time between retries grows exponentially.

The default behavior for this is as follows:

--backoff-factor 

A multiplier that scales how long the waits are. With exponential backoff, the wait before attempt n is roughly backoff-factor * (2 ** n). So factor=1 gives you waits of 1, 2, 4, 8, 16… seconds. Bump it to backoff-factor=2 and you get 2, 4, 8, 16…; drop it to backoff-factor=0.5 and you get 0.5, 1, 2, 4…. Think of it as a dial that stretches or shrinks the whole waiting curve up and down.

--backoff-max-value

A ceiling on any single wait. Exponential growth gets big fast (1, 2, 4… 512, 1024 seconds), which you usually don't want. backoff-max-value caps how long any one wait can be. With backoff-max-value=60, the waits climb 1, 2, 4, 8, 16, 32, then flatten out at 60, 60, 60, instead of continuing to double.

--backoff-max-tries

The total number of attempts before backoff gives up and lets the error through. It counts all tries, including the very first one.

Put together, a typical setup like backoff-factor=1, backoff-max-value=60, backoff-max-tries=5 means: retry up to 4 times, waiting 1s, then 2s, 4s, 8s — but never longer than 60s on any single wait.

Defaults are backoff-factor=5.0, backoff-max-value=30, backoff-max-tries=5

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


# export-datasets
Command and to dump Butler dataset, dimension, and calibration validity range data to a YAML file.

This command works alongside "rucio-register".
It can be used to record all the files registered into Rucio so that their transfer and ingestion at the destination can be confirmed.
In addition, it preserves dimension data and calibration validity range data that is not otherwise transferred via Rucio.
This additional data can be useful for repeated ingests of raw and calibration data into Butler repositories.

## Examples

To record the dimension values (notably _not_ including the visit dimension, which would have to be regenerated) for a set of raw images:

```
export-datasets \
    --root /sdf/group/rubin/lsstdata/offline/instrument/ \
    --filename Dataset-LSSTCam-NoTract-20250101-0000.yaml \
    --collections LSSTCam/raw/all \
    --where "instrument='LSSTCam' and day_obs=20250101 and exposure.seq_num IN (1..99)" \
    --limit 30000 \
    /repo/main raw
```
`--root` is needed here since the original files are ingested as full URLs with `direct`.

To record the datasets created by a multi-site processing workflow:

```
export-datasets \
    --filename Dataset-LSSTCam-Tract2024-Step3-Group5-metadata.yaml \
    --collections step3/group5 \
    --where "tract=2024" \
    $LOCAL_REPO '*_metadata'
```
Note the use of a glob pattern to select dataset types of interest.

