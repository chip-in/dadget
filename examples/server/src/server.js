import { ResourceNode } from '@chip-in/resource-node';
import {
  DatabaseRegistry
  , ContextManager
  , UpdateManager
  , QueryHandler
  , SubsetStorage} from '@chip-in/dadget';
import fs from 'fs'

let env = {}
try {
  let text = fs.readFileSync('/proc/1/environ', { encoding: 'utf-8' });
  let envList = text.split("\0");
  envList.forEach(row => {
    let data = row.split("=", 2);
    env[data[0]] = data[1];
  })
} catch (e) {}

const CORE_SERVER = process.env.CORE_SERVER ? process.env.CORE_SERVER : env.CORE_SERVER ? env.CORE_SERVER : "http://test-core.chip-in.net";
const RN_NAME = process.env.RN_NAME ? process.env.RN_NAME : env.RN_NAME ? env.RN_NAME : "db-server";

let node = new ResourceNode(CORE_SERVER, RN_NAME);
node.registerServiceClasses({
  DatabaseRegistry,
  ContextManager,
  UpdateManager,
  QueryHandler,
  SubsetStorage
});
node.start().then(() => {
  process.on('SIGINT', function () {
    node.stop().then(()=>{
      process.exit()
    })
  })
})

