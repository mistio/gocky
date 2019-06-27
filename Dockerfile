FROM golang:1.10-alpine

RUN apk add --update --no-cache git

COPY . /go/src/github.com/mistio/gocky

RUN mkdir -p /go/src/github.com/influxdata

WORKDIR /go/src/github.com/influxdata

RUN git clone https://github.com/influxdata/influxdb

WORKDIR /go/src/github.com/influxdata/influxdb

RUN git checkout v1.7.7

WORKDIR /go/src/github.com/mistio/gocky

RUN go get

RUN go build

ENTRYPOINT ["/go/src/github.com/mistio/gocky/gocky"] 
