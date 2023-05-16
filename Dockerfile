# syntax=docker/dockerfile:experimental
#FROM minhhoangdang/ubuntu2004-dind:amd64
FROM minhhoangdang/ubuntu2004-dind:arm64
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y git libboost-all-dev openjdk-11-jdk \
    python3-pip python3-wheel python3-dev wbritish wamerican \
    python-is-python3 build-essential wget curl ruby \
    && apt-get clean && rm -rf /var/cache/apt/archives /var/lib/apt/lists

WORKDIR /FedShop
# COPY . /FedShop/

# Clone the repo
RUN git clone --recurse-submodule https://github.com/mhoangvslev/FedShop.git /FedShop
RUN pip install --no-cache Cython && pip install --no-cache git+https://github.com/oddconcepts/n2o.git
RUN pip install --no-cache -r requirements.txt
RUN python -m nltk.downloader stopwords punkt

# Install dependencies
RUN curl -s https://bitbucket.org/mjensen/mvnvm/raw/master/mvn > /usr/bin/mvn && chmod a+x /usr/bin/mvn
RUN wget https://github.com/docker/compose/releases/download/v2.16.0/docker-compose-linux-x86_64 -O /usr/bin/docker-compose && chmod a+x /usr/bin/docker-compose

WORKDIR /FedShop/generators/watdiv
RUN make rebuild && make install

VOLUME /FedShop/experiments /FedShop/engines /FedShop/rsfb
WORKDIR /FedShop
ENTRYPOINT [ "dockerd-entrypoint.sh" ]
CMD []
