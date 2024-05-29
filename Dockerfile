FROM golang:1.22.0 as build

WORKDIR /

COPY ./cert ./cert

COPY ./build/mqtt-kafka-bridge-linux-amd64 mqtt-kafka-bridge

ENTRYPOINT ["/mqtt-kafka-bridge"]

