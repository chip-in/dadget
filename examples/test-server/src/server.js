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
//  .then(_=>forwardQueryTest(_.csn))
  .then(()=>insertDemo(new Date()))
  .then(_=>updateDemo(_))
//  .then(_=>updateDemo(_))
//  .then(_=>deleteDemo(_))
//  .then(queryTest)

  function queryTest(){
    return dadget.query({ alertClass: "EvacuationOrder", date: { $gt: "2017-08-07T10:23:00" } })
    .then((result) => {
      console.log("queryTest:", JSON.stringify(result))
      for(let row of result.resultSet){
        console.log(JSON.stringify(row))
      }
      return result
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

  function forwardQueryTest(csn){
    csn++;
    let query = { alertClass: "EvacuationOrder", date: { $gt: "2017-08-07T10:23:00" } };
    let queryHandlers = node.searchServiceEngine("QueryHandler", { database: "alerts" }).sort((a, b) => b.getPriority() - a.getPriority());
    let resultSet = []
    return Promise.resolve({ csn: csn, resultSet: resultSet, restQuery: query, queryHandlers: queryHandlers })
      .then(function queryFallback(request){
        if (!Object.keys(request.restQuery).length) return Promise.resolve(request);
        let qh = request.queryHandlers.shift();
        if (qh == null) {
          throw new Error("The queryHandlers has been empty before completing queries.");
        }
        return qh.query(request.csn, request.restQuery)
          .then((result) => queryFallback({
            csn: result.csn,
            resultSet: [...request.resultSet, ...result.resultSet],
            restQuery: result.restQuery,
            queryHandlers: request.queryHandlers
          }));
      }).then(result => {
        console.log("forwardQueryTest:", JSON.stringify(result))
      })
    
  }
})

