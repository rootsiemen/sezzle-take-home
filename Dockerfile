FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY . ./

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/weatherlookup .

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/weatherlookup /weatherlookup

USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/weatherlookup"]
