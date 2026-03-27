FROM golang:1.25

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY go.mod ./
RUN go mod download

COPY . .

RUN go build -o execfile

CMD ["./execfile"]