import * as http from 'http';
import * as EJSON from '../util/Ejson'
import { DadgetError } from './DadgetError';
import { ERROR } from '../Errors';
import { getAccessControlAllowOrigin } from '../Config'

export class ProxyHelper {

  static procPost(req: http.IncomingMessage, res: http.ServerResponse, proc: (data: string) => object): Promise<http.ServerResponse> {
    let head: any = {
      "Content-Type": "application/json"
    }
    let allowOrigin = getAccessControlAllowOrigin(req.headers["origin"] as string)
    if (allowOrigin) {
      head["Access-Control-Allow-Origin"] = allowOrigin
    }
    let promise = new Promise<string>((resolve, reject) => {
      var postData = "";
      req.on("data", function (chunk) {
        postData += chunk;
      })
      req.on("end", function () {
        console.log("procPost receive:", postData)
        resolve(postData)
      })
      req.on("error", function (error) {
        reject(error)
      })
    })
      .then(proc)
      .then((result) => {
        //        console.log("procPost return", JSON.stringify(result))
        res.writeHead(200, head)
        res.write(EJSON.stringify(result))
        res.end()
        return res
      })
      .catch(reason => {
        console.log("procPost error return", reason.toString())
        if (!(reason instanceof DadgetError)) {
          reason = new DadgetError(ERROR.E3001, [reason])
        }
        reason.convertInsertsToString()
        res.writeHead(200, head)
        res.write(EJSON.stringify({ status: "NG", reason: reason }))
        res.end()
        return res
      })
    return promise
  }

  static procOption(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    let head: any = {
      "Access-Control-Allow-Methods": "POST, GET, OPTIONS"
      , "Access-Control-Allow-Headers": "Content-Type"
    }
    let allowOrigin = getAccessControlAllowOrigin(req.headers["origin"] as string)
    if (allowOrigin) {
      head["Access-Control-Allow-Origin"] = allowOrigin
    }
    res.writeHead(200, head)
    res.end()
    return Promise.resolve(res)
  }

  static procError(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    res.writeHead(404);
    res.write("not found!");
    res.end();

    return Promise.resolve(res)
  }
}