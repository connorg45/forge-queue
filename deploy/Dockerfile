FROM golang:1.26.5-alpine AS build

RUN apk add --no-cache ca-certificates git
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/forge-api ./cmd/forge-api
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/forge-worker ./cmd/forge-worker
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/forge-scheduler ./cmd/forge-scheduler
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/forge ./cmd/forge-cli

FROM alpine:3.20

RUN apk add --no-cache ca-certificates && adduser -D -H forge
COPY --from=build /out/forge-api /bin/forge-api
COPY --from=build /out/forge-worker /bin/forge-worker
COPY --from=build /out/forge-scheduler /bin/forge-scheduler
COPY --from=build /out/forge /bin/forge
USER forge
ENTRYPOINT ["/bin/forge-api"]
