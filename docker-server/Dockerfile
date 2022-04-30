FROM node:14-alpine

ENV CI_HOME=/usr/local/chip-in

RUN apk update
COPY . ${CI_HOME}
WORKDIR ${CI_HOME}/docker-server
RUN npm set unsafe-perm true
RUN npm install
RUN npm run build
ENTRYPOINT ["node", "lib/server.js"]
