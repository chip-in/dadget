export class DadgetError {
  public code: number;
  public message: string;

  constructor(
    err: { code: number, message: string },
    public inserts: Array<object | string | number> = [],
    public ns: string = "dadget.chip-in.net") {

    this.code = err.code;
    this.message = err.message;
  }

  static from(from: any): DadgetError {
    return new DadgetError({ code: from.code, message: from.message }, from.inserts, from.ns);
  }

  convertInsertsToString() {
    this.inserts = this.inserts.map((v) => {
      if (typeof v === "string") { return v; }
      if (v.toString) { return v.toString(); }
      return JSON.stringify(v);
    });
  }

  toString(): string {
    this.convertInsertsToString();
    return this.message.replace(/%([\d]+)/g, (match, i) => this.inserts && this.inserts[i - 1] ? this.inserts[i - 1].toString() : "undefined");
  }
}
