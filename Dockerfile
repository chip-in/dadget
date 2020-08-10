FROM node:8-alpine

ENV CI_HOME=/usr/local/chip-in/dadget

RUN apk update
COPY . ${CI_HOME}
RUN cd ${CI_HOME} && npm install
WORKDIR ${CI_HOME}
ENTRYPOINT ["node", "lib/command.js"]
