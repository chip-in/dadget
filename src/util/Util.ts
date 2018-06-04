import * as parser from "mongo-parse";

export class Util {

  static promiseWhile<T>(
    data: T,
    condition: (data: T) => boolean,
    action: (data: T) => Promise<T>,
  ): Promise<T> {
    const whilst = (data: T): Promise<T> => {
      return condition(data) ? action(data).then(whilst) : Promise.resolve(data);
    };
    return whilst(data);
  }

  static mongoSearch(documents: any[], query: object, sort?: any, validate?: boolean): any {
    const parsedQuery = parser.parse(query);
    return documents.filter((doc) => {
      return parsedQuery.matches(doc, validate);
    }).sort((a, b) => {
      if (!sort) { return 0; }
      // tslint:disable-next-line:forin
      for (const k in sort) {
        let result = sortCompare(a, b, k);
        if (result !== 0) {
          if (sort[k] < 0) {
            result = -result;
          }
          return result;
        }
      }
      return 0;
    });

    function sortCompare(a: any, b: any, sortProperty: any) {
      const aVal = parser.DotNotationPointers(a, sortProperty)[0].val;
      const bVal = parser.DotNotationPointers(b, sortProperty)[0].val;
      if (aVal === null || typeof aVal === "undefined") {
        if (bVal === null || typeof bVal === "undefined") {
          return 0;
        } else {
          return -1;
        }
      }
      if (bVal === null || typeof bVal === "undefined") {
        return 1;
      }
      if (aVal > bVal) {
        return 1;
      } else if (aVal < bVal) {
        return -1;
      } else {
        return 0;
      }
    }
  }
}
