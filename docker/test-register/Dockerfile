ARG LSST_STACK_VERSION
FROM lsstsqre/centos:${LSST_STACK_VERSION}

WORKDIR /home/lsst
RUN source /opt/lsst/software/stack/loadLSST.bash && \
    pip install rucio-clients

COPY setup.sh /home/lsst/setup.sh

ENTRYPOINT [ "/usr/bin/tail", "-f", "/dev/null" ]
