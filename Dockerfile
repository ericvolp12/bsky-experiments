FROM golang:1.20

WORKDIR /app
COPY docker_install.sh .

RUN chmod +x docker_install.sh
RUN bash docker_install.sh

RUN go install github.com/kyleconroy/sqlc/cmd/sqlc@latest

COPY . .

