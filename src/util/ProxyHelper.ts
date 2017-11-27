import * as http from 'http';
import * as EJSON from 'mongodb-extended-json'

export class ProxyHelper {

  static procPost(req: http.IncomingMessage, res: http.ServerResponse, proc: (data: string) => object): Promise<http.ServerResponse> {
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
        res.writeHead(200, {
          "Content-Type": "application/json"
          , "Access-Control-Allow-Origin": "*"
        })
        res.write(EJSON.stringify(result))
        res.end()
        return res
      })
      .catch(reason => {
        console.log("procPost error return", JSON.stringify(reason))
        res.writeHead(200, {
          "Content-Type": "application/json"
          , "Access-Control-Allow-Origin": "*"
        })
        res.write(EJSON.stringify({status: "NG", reason: reason}))
        res.end()
        return res
      })
    return promise
  }

  static procOption(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    res.writeHead(200, {
      "Access-Control-Allow-Origin": "*"
      , "Access-Control-Allow-Methods": "POST, GET, OPTIONS"
      , "Access-Control-Allow-Headers": "Content-Type"
    })
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