FROM golang:1.13-buster

RUN apt update && apt install -y build-essential && \
    wget https://www.foundationdb.org/downloads/6.2.7/ubuntu/installers/foundationdb-clients_6.2.7-1_amd64.deb && \
    dpkg -i foundationdb-clients_6.2.7-1_amd64.deb && \
    rm foundationdb-clients_6.2.7-1_amd64.deb && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /go/src/github.com/influxdata /go/src/github.com/apple /go/src/github.com/wavefronthq

WORKDIR /go/src/github.com/influxdata

RUN git clone https://github.com/influxdata/influxdb

WORKDIR /go/src/github.com/influxdata/influxdb

RUN git checkout v1.7.7

WORKDIR /go/src/github.com/apple

RUN git clone https://github.com/apple/foundationdb

WORKDIR /go/src/github.com/apple/foundationdb

RUN git checkout 6.2.7

WORKDIR /go/src/github.com/wavefronthq/

RUN git clone https://github.com/wavefrontHQ/wavefront-sdk-go

WORKDIR /go/src/github.com/wavefronthq/wavefront-sdk-go

RUN git checkout v0.9.1

COPY . /go/src/github.com/mistio/gocky

WORKDIR /go/src/github.com/mistio/gocky

RUN go get

RUN go build

ENTRYPOINT ["/go/src/github.com/mistio/gocky/gocky"]
