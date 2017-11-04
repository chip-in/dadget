import { ResourceNode } from '@chip-in/resource-node';
import {
  DatabaseRegistry
  , ContextManager
  , UpdateManager
  , QueryHandler
  , SubsetStorage} from '@chip-in/dadget';

const CORE_SERVER = "http://test-core.chip-in.net";
const RN_NAME = "db-server";

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

