import { ResourceNode } from '@chip-in/resource-node';
import {
  DatabaseRegistry
  , ContextManager
  , UpdateManager
  , QueryHandler
  , SubsetStorage
  , Dadget} from '../../..';

let node = new ResourceNode("http://test-core.chip-in.net", "db-server-test");
node.registerServiceClasses({
  DatabaseRegistry,
  ContextManager,
  Dadget,
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
  let seList = node.searchServiceEngine("Dadget", { database: "alerts" });
  if (seList.length != 1) {
    //TODO エラー処理
    throw new Error("Dadgetエラー:" + seList.length)
  }
  let dadget = seList[0];
  queryTest()
  .then(()=>insertDemo("ddddddddd"))
  .then(_=>updateDemo(_))
  .then(_=>deleteDemo(_))
  .then(queryTest)

  function queryTest(){
    return dadget.query({ alertClass: "EvacuationOrder", date: { $gt: "2017-08-07T10:23:00" } })
    .then((result) => {
      console.log("queryTest:", JSON.stringify(result))
      for(let row of result.resultSet){
        console.log(JSON.stringify(row))
      }
    });
  }

  function insertDemo(data) {
    let id = Dadget.uuidGen();
    return dadget.exec(0, {
      type: "insert",
      target: id,
      new: {
        // 任意のオブジェクト
        alertClass: "EvacuationOrder",
        date: "2017-08-07T10:23:24",
        title: data,
        distributionId: Dadget.uuidGen()
      }})
      .then(result => {
        console.log("insertDemo succeeded:", JSON.stringify(result))
        return result
      })
      .catch(reason => {
        console.log("insertDemo faild", reason)
      })
  }

  function updateDemo(obj) {
    console.log("updateDemo:", JSON.stringify(obj))
    return dadget.exec(obj.csn, {
      type: "update",
      target: obj._id,
      before: obj,
      operator: {
        "$set": {
          "setval": "test"
        }
      }})
      .then(result => {
        console.log("updateDemo succeeded:", JSON.stringify(result))
        return result
      })
      .catch(reason => {
        console.log("updateDemo faild", reason)
      })
  }

  function deleteDemo(obj) {
    console.log("deleteDemo:", JSON.stringify(obj))
    return dadget.exec(obj.csn, {
      type: "delete",
      target: obj._id,
      before: obj
    })
      .then(result => {
        console.log("deleteDemo succeeded:", JSON.stringify(result))
        return result
      })
      .catch(reason => {
        console.log("deleteDemo faild", reason)
      })
  }
})

