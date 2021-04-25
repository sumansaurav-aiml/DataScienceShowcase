FROM oraclelinux:8-slim
RUN microdnf update -y && microdnf install -y oraclelinux-release-el8 && \
    microdnf install wget -y && microdnf install python38 libaio -y && \
    wget https://download.oracle.com/otn_software/linux/instantclient/211000/oracle-instantclient-basic-21.1.0.0.0-1.x86_64.rpm && \
                    rpm -i oracle-instantclient-basic-21.1.0.0.0-1.x86_64.rpm && microdnf clean all
RUN wget https://bootstrap.pypa.io/get-pip.py && python3.8 get-pip.py && \
    microdnf install gcc python38-devel  python38-pip -y

WORKDIR /usr/src/app

COPY . .

RUN pip3.8 install -r requirements.txt && \
    pip3.8 install awscli && \
    mkdir -p data && \
    chmod +x start.sh

ENTRYPOINT /usr/src/app/start.sh