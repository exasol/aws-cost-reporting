FROM debian:stable
MAINTAINER Marc Popp (marc.popp@exasol.com)
WORKDIR /app
ENTRYPOINT bash -c "echo 'Container Started'; while true; do sleep 30; done;"

# ENV TERM=xterm
# ENV LC_ALL=en_US.UTF-8
# ENV LANG=en_US.UTF-8
# ENV LANGUAGE=en_US.UTF-8
# RUN localedef -i en_US -f UTF-8 en_US.UTF-8

RUN apt-get -y update && \
    apt-get install -y selinux-utils policycoreutils locales apt apt-utils aptitude unattended-upgrades ca-cacert && \
    apt-get install -y awscli python3-pip && \
    apt-get install -y python3-pyodbc unixodbc && \
    apt-get -yq clean && \
    python3 -m pip install ExasolDatabaseConnector && \
    python3 -m pip install boto3 && \
    python3 -m pip install skew && \
    rm -rf /root/.cache

