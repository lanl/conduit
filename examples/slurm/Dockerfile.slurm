ARG SLURM_VER="24.05.8"

FROM conduit-cmd:latest AS conduit-cmd

FROM slurmctld:${SLURM_VER}

RUN adduser -u 1433 testuser

RUN dnf update -y && \
    dnf install -y git lua-posix wget tar gzip make && \
    dnf clean all

RUN ln -s /etc/conduit/burst_buffer.lua /usr/local/etc/burst_buffer.lua && \
    ln -s /etc/conduit/burst_buffer.conf /usr/local/etc/burst_buffer.conf

COPY slurm-entrypoint.sh /etc/slurm_docker/

COPY --from=conduit-cmd /sbin/conduit /usr/bin/conduit

ENTRYPOINT ["/etc/slurm_docker/slurm-entrypoint.sh"]
