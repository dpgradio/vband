FROM golang:1.23 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY vband.go .

# CGO - disabled so that it is built in pure go
# GOOS - target linux
# GOARCH - architecture
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o vband .


CMD ["./vband"]
