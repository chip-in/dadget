import * as http from "http";
import { getAccessControlAllowOrigin } from "../Config";
import { ERROR } from "../Errors";
import * as EJSON from "../util/Ejson";
import { DadgetError } from "./DadgetError";

export class ProxyHelper {

  static procPost(
    req: http.IncomingMessage,
    res: http.ServerResponse,
    proc: (data: string) => object): Promise<http.ServerResponse> {

    const head: any = {
      "Content-Type": "application/json",
    };
    const allowOrigin = getAccessControlAllowOrigin(req.headers.origin as string);
    if (allowOrigin) {
      head["Access-Control-Allow-Origin"] = allowOrigin;
    }
    const promise = new Promise<string>((resolve, reject) => {
      let postData = "";
      req.on("data", (chunk) => {
        postData += chunk;
      });
      req.on("end", () => {
        console.log("procPost receive:", postData);
        resolve(postData);
      });
      req.on("error", (error) => {
        reject(error);
      });
    })
      .then(proc)
      .then((result) => {
        //        console.log("procPost return", JSON.stringify(result))
        res.writeHead(200, head);
        res.write(EJSON.stringify(result));
        res.end();
        return res;
      })
      .catch((reason) => {
        console.log("procPost error return", reason.toString());
        if (!(reason instanceof DadgetError)) {
          reason = new DadgetError(ERROR.E3001, [reason]);
        }
        reason.convertInsertsToString();
        res.writeHead(200, head);
        res.write(EJSON.stringify({ status: "NG", reason }));
        res.end();
        return res;
      });
    return promise;
  }

  static procOption(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    const head: any = {
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
    };
    const allowOrigin = getAccessControlAllowOrigin(req.headers.origin as string);
    if (allowOrigin) {
      head["Access-Control-Allow-Origin"] = allowOrigin;
    }
    res.writeHead(200, head);
    res.end();
    return Promise.resolve(res);
  }

  static procError(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    res.writeHead(404);
    res.write("not found!");
    res.end();

    return Promise.resolve(res);
  }
}
