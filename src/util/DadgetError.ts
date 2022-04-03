export class DadgetError {
  public code: number;
  public message: string;

  constructor(
    err: { code: number, message: string },
    public inserts: (object | string | number)[] = [],
    public ns: string = "dadget.chip-in.net") {

    this.code = err.code;
    this.message = err.message;
  }

  static from(from: any): DadgetError {
    const message = from.message ? from.message : (from.toString ? from.toString() : from.code);
    return new DadgetError({ code: from.code, message }, from.inserts, from.ns);
  }

  convertInsertsToString() {
    this.inserts = this.inserts.map((v) => {
      if (typeof v === "string") { return v; }
      if (v.toString) { return v.toString(); }
      return JSON.stringify(v);
    });
  }

  toString(): string {
    if (!this.message) { return this.code ? this.code.toString() : "no message"; }
    if (!this.message.replace) { this.message = this.message.toString(); }
    this.convertInsertsToString();
    return this.message.replace(/%([\d]+)/g, (match, i) => this.inserts && this.inserts[i - 1] ? this.inserts[i - 1].toString() : "undefined");
  }
}

export class UniqueError extends Error {
  constructor(e: string, public obj: object) {
    super(e);
    this.name = new.target.name;
  }
}