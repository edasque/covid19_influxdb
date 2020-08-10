go get github.com/influxdata/influxdb1-client github.com/influxdata/influxdb1-client/v2

GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build  -o ingestCV19data.linux.amd64 ingestCV19data.go

# docker build -t ingestcv19data .

# docker run -v config.json /home/config.json
