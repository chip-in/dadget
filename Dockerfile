FROM node:14-alpine

ENV CI_HOME=/usr/local/chip-in/dadget

RUN apk update
COPY . ${CI_HOME}
RUN npm set unsafe-perm true
RUN cd ${CI_HOME} && npm install
WORKDIR ${CI_HOME}
ENTRYPOINT ["node", "lib/command.js"]
