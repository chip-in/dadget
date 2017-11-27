declare module "mongodb-extended-json" {
  export function stringify(obj: object): string;
  export function parse(str: string): any;
  export function deserialize(data: any): any;
}