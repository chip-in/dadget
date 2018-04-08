declare module "mongo-parse" {
  class Parse {
    map(callback: (key: string, value: any) => object): object;
    mapValues(callback: (field: string, value: any) => object): object;
    matches(document: object, validate?: boolean): boolean;
  }

  export function parse(mongoQuery: object): Parse;
  export function inclusive(mongoProjection: object): boolean;
  export function search(documents: any[], query: object, sort?: object, validate?: boolean): any;
}
