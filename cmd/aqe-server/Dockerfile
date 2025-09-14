FROM golang:1.24-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
WORKDIR /app/cmd/aqe-server
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /aqe-server

FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=build /aqe-server /app/aqe-server
EXPOSE 8080
ENV PORT=8080
CMD ["/app/aqe-server"]
