ARG LSST_STACK_VERSION
FROM ghcr.io/lsst/scipipe:al9-w_2025_25

WORKDIR /home/lsst
RUN source /opt/lsst/software/stack/loadLSST.bash && \
    mamba install rucio-clients

COPY setup.sh /home/lsst/setup.sh

ENTRYPOINT [ "/usr/bin/tail", "-f", "/dev/null" ]
