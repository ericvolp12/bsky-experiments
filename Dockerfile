FROM golang:1.20

WORKDIR /app
COPY docker_install.sh .

RUN chmod +x docker_install.sh
RUN bash docker_install.sh

COPY . .

#CMD ["/bin/bash", "-c", "service docker start && make graph-builder-up"]
# && make graph-builder-up"]
# && export DOCKER_HOST='tcp://0.0.0.0:2375' &
