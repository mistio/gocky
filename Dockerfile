FROM golang:1.11-alpine

RUN apk add --update --no-cache git

COPY . /go/src/github.com/mistio/gocky

WORKDIR /go/src/github.com/mistio/gocky

RUN go get

RUN go build

ENTRYPOINT ["/go/src/github.com/mistio/gocky/gocky"] 
