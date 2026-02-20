FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libaio1t64 \
    curl \
    unzip \
    git \
    wget \
    libssl-dev \
    libffi-dev \
    python3-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1

WORKDIR /opt/oracle

ADD https://download.oracle.com/otn_software/linux/instantclient/1925000/instantclient-basic-linux.x64-19.25.0.0.0dbru.zip /opt/oracle/instantclient.zip

RUN unzip instantclient.zip \
    && rm instantclient.zip \
    && sh -c "echo /opt/oracle/instantclient_19_25 > /etc/ld.so.conf.d/oracle-instantclient.conf" \
    && ldconfig

ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_19_25
ENV TNS_ADMIN=/opt/oracle/instantclient_19_25/network/admin
ENV PATH=$PATH:/opt/oracle/instantclient_19_25

WORKDIR /app

RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x start.sh

EXPOSE 8501

CMD ["tail", "-f", "/dev/null"]
