import { ResourceNode } from '@chip-in/resource-node';
import Dadget from '../../..';
import fs from 'fs'

let env = {}
try {
  let text = fs.readFileSync('/proc/1/environ', { encoding: 'utf-8' });
  let envList = text.split("\0");
  envList.forEach(row => {
    let data = row.split("=", 2);
    env[data[0]] = data[1];
  })
} catch (e) { }

const CORE_SERVER = process.env.CORE_SERVER ? process.env.CORE_SERVER : env.CORE_SERVER ? env.CORE_SERVER : "http://core";
const RN_NAME = process.env.RN_NAME ? process.env.RN_NAME : env.RN_NAME ? env.RN_NAME : "db-server";

let node = new ResourceNode(CORE_SERVER, RN_NAME);
if (process.env.ACCESS_TOKEN) {
  node.setJWTAuthorization(process.env.ACCESS_TOKEN);
}
let Logger = Dadget.getLogger();
Logger.setLogLevel(process.env.LOG_LEVEL || "debug");
//Dadget.getLogger().setLogLevel('debug');
Dadget.registerServiceClasses(node);

setInterval(() => {
  try {
    console.log(new Date(), 'begin gc');
    global.gc();
  } catch (e) {
    console.error(new Date(), 'gc failed');
  }
}, 1 * 60 * 1000);

node.start().then(() => {
  function sigHandle() {
    node.stop().then(() => {
      process.exit()
    })
      .catch((msg) => {
        console.error('\u001b[31m' + (msg.toString ? msg.toString() : msg) + '\u001b[0m');
        process.exit(1);
      })
  }
  process.on('SIGINT', sigHandle);
  process.on('SIGTERM', sigHandle);
})
  .catch((msg) => {
    console.error('\u001b[31m' + (msg.toString ? msg.toString() : msg) + '\u001b[0m');
    process.exit(1);
  })


