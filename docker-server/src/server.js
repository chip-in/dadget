import { ResourceNode } from '@chip-in/resource-node';
import { LogUploader } from '@chip-in/logger'
import Dadget from '../..';
import memwatch from '@airbnb/node-memwatch';

if (!process.env.CORE_SERVER) {
  console.error("CORE_SERVER required");
  process.exit();
}
if (!process.env.RN_NAME) {
  console.error("RN_NAME required");
  process.exit();
}

let rnode = new ResourceNode(process.env.CORE_SERVER, process.env.RN_NAME);
let Logger = Dadget.getLogger();
Logger.setLogLevel(process.env.LOG_LEVEL || "info");
Logger.setMaxStringLength(process.env.LOG_MAX_LENGTH || 1024);
if (process.env.USE_LOG_UPLOADER) {
  LogUploader.registerServiceClasses(rnode);
  Logger.attachUploader(rnode)
}

Dadget.registerServiceClasses(rnode);

let jwtToken = process.env.ACCESS_TOKEN;
let jwtRefreshPath = process.env.TOKEN_UPDATE_PATH;
if (jwtToken) {
  rnode.setJWTAuthorization(jwtToken, jwtRefreshPath);
}

let hd = new memwatch.HeapDiff();
setInterval(() => {
  try {
    console.log(new Date(), 'begin gc');
    global.gc();
    const used = process.memoryUsage();
    const messages = [];
    for (let key in used) {
      messages.push(`${key}: ${Math.round(used[key] / 1024 / 1024 * 100) / 100} MB`);
    }
    console.log(new Date(), messages.join(', '));
    console.dir(hd.end(), {depth: 3})
    hd = new memwatch.HeapDiff();
  } catch (e) {
    console.error(new Date(), 'gc failed');
  }
}, 10 * 60 * 1000);

rnode.start().then(() => {
  function sigHandle() {
    rnode.stop().then(() => {
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
