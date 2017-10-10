FROM golang:1.8-alpine

RUN apk add --update --no-cache git

COPY . /go/src/github.com/mistio/gocky

WORKDIR /go/src/github.com/mistio/gocky

RUN go get

RUN go-wrapper install

CMD ["go-wrapper", "run"] # ["app"]
