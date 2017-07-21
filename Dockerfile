FROM golang:1.8

COPY gocky /gocky

COPY compose.toml /config.toml

CMD /gocky -config /config.toml
