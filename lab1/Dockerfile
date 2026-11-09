FROM golang:1.25

WORKDIR /app

COPY main.go broker_handlers.go broker.go ./

RUN go build -o http_server main.go broker.go broker_handlers.go

RUN mkdir -p public

EXPOSE 8080

CMD ["./http_server"," 8080"]

