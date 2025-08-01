FROM node:22-alpine

ENV CI_HOME=/usr/local/chip-in

RUN apk update
COPY . ${CI_HOME}
WORKDIR ${CI_HOME}
RUN npm install
RUN npm run clean
RUN npm run release_build
RUN chmod +x bin/command
WORKDIR ${CI_HOME}/docker-server
RUN npm install
RUN npm run build
ENTRYPOINT ["node", "--expose-gc", "lib/server.js"]
