FROM alpine:3
WORKDIR /home
COPY ingestCV19data.linux.amd64 ingestCV19data.linux.amd64

CMD while true; do ./ingestCV19data.linux.amd64; sleep 3600; done