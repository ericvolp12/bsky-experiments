FROM node:18 as exporter

WORKDIR /app

COPY package.json tsconfig.json yarn.lock ./

RUN yarn install --frozen-lockfile

COPY src ./src

RUN yarn build

# Volume mount /public inside the container to ../public in the current directory
# to make the generated files available to the host.
VOLUME /public

CMD ["yarn", "start"]
