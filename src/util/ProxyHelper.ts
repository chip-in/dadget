import { Logger } from "@chip-in/resource-node";
import * as http from "http";
import * as URL from "url";
import { getAccessControlAllowOrigin } from "../Config";
import { ERROR } from "../Errors";
import * as EJSON from "../util/Ejson";
import { DadgetError } from "./DadgetError";

export class ProxyHelper {

  static procGet(
    req: http.IncomingMessage,
    res: http.ServerResponse,
    logger: Logger,
    proc: (data: any) => object): Promise<http.ServerResponse> {

    const head: any = {
      "Content-Type": "application/json",
      "Cache-Control": "no-cache",
    };
    const allowOrigin = getAccessControlAllowOrigin(req.headers.origin as string);
    if (allowOrigin) {
      head["Access-Control-Allow-Origin"] = allowOrigin;
    }
    if (!req.url) { throw new Error("no url"); }
    const url = URL.parse(req.url, true);
    const promise = Promise.resolve(proc(url.query))
      .then((result) => {
        res.writeHead(200, head);
        res.write(EJSON.stringify(result));
        res.end();
        return res;
      })
      .catch((reason) => {
        logger.warn("procGet error return", reason.toString());
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

  static procPost(
    req: http.IncomingMessage,
    res: http.ServerResponse,
    logger: Logger,
    proc: (data: any) => object): Promise<http.ServerResponse> {

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
        resolve(postData);
      });
      req.on("error", (error) => {
        reject(error);
      });
    })
      .then(EJSON.parse)
      .then(proc)
      .then((result) => {
        res.writeHead(200, head);
        res.write(EJSON.stringify(result));
        res.end();
        return res;
      })
      .catch((reason) => {
        logger.warn("procPost error return", reason.toString());
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

  static validateNumber(val: any, name: string): number | undefined {
    if (typeof val === "undefined" || val === "" || val === null) { return undefined; }
    if (!isFinite(val)) { throw new Error(name + " should be number."); }
    return Number(val);
  }

  static validateNumberRequired(val: any, name: string): number {
    if (typeof val === "undefined" || val === "" || val === null) { throw new Error(name + " is required."); }
    if (!isFinite(val)) { throw new Error(name + " should be number."); }
    return Number(val);
  }
}
