import equal = require("deep-equal");

const showLog = false;

export class LogicalOperator {
  static getInsideOfCache(cond: any, cacheCond: any): object | undefined {
    if (cacheCond === undefined) { return cond; }
    let query: Operator;
    indent = 0;
    let result = Operator.fromMongo({ $and: [cond, cacheCond] });
    do {
      query = result;
      result = expandLogicalQuery(query);
    } while (!equal(result.toMongo(), query.toMongo()));
    if (result instanceof FalseOperator) { return undefined; }
    if (result instanceof TrueOperator) { return cond; }
    result = shrinkLogicalQuery(result);
    return Operator.toMongo(result);
  }

  static getOutsideOfCache(cond: any, cacheCond: any): object | undefined {
    if (cacheCond === undefined) { return undefined; }
    let query: Operator;
    indent = 0;
    let result = Operator.fromMongo({ $and: [cond, { $not: cacheCond }] });
    do {
      query = result;
      result = expandLogicalQuery(query);
    } while (!equal(result.toMongo(), query.toMongo()));
    if (result instanceof FalseOperator) { return undefined; }
    if (result instanceof TrueOperator) { return cond; }
    result = shrinkLogicalQuery(result);
    return Operator.toMongo(result);
  }
}

const parserMap: { [key: string]: (query: any) => Operator } = {};

parserMap.$and = (query: any): Operator => {
  if (query instanceof Array) {
    return new AndOperator(query.map((val) => Operator.fromMongo(val)));
  }
  throw new Error("Not supported query:" + JSON.stringify(query));
};

parserMap.$not = (query: any): Operator => {
  return new NotOperator(Operator.fromMongo(query));
};

parserMap.$nor = (query: any): Operator => {
  if (query instanceof Array) {
    return new NotOperator(new OrOperator(query.map((val) => Operator.fromMongo(val))));
  }
  throw new Error("Not supported query:" + JSON.stringify(query));
};

parserMap.$or = (query: any): Operator => {
  if (query instanceof Array) {
    return new OrOperator(query.map((val) => Operator.fromMongo(val)));
  }
  throw new Error("Not supported query:" + JSON.stringify(query));
};

const comparisonParserMap: { [key: string]: (field: string, val: any, wholeQuery: any) => Operator | false } = {};

comparisonParserMap.$eq = (field: string, val: any, wholeQuery: any): Operator | false => {
  return new EqOperator(field, val);
};

comparisonParserMap.$gt = (field: string, val: any, wholeQuery: any): Operator | false => {
  return new GtOperator(field, val);
};

comparisonParserMap.$gte = (field: string, val: any, wholeQuery: any): Operator | false => {
  return new GteOperator(field, val);
};

comparisonParserMap.$in = (field: string, val: any, wholeQuery: any): Operator | false => {
  if (val instanceof Array) { return new InOperator(field, val); }
  throw new Error("Not supported query:" + JSON.stringify(wholeQuery));
};

comparisonParserMap.$lt = (field: string, val: any, wholeQuery: any): Operator | false => {
  return new LtOperator(field, val);
};

comparisonParserMap.$lte = (field: string, val: any, wholeQuery: any): Operator | false => {
  return new LteOperator(field, val);
};

comparisonParserMap.$ne = (field: string, val: any, wholeQuery: any): Operator | false => {
  return new NotValueOperator(field, new EqOperator(field, val));
};

comparisonParserMap.$nin = (field: string, val: any, wholeQuery: any): Operator | false => {
  if (val instanceof Array) { return new NotValueOperator(field, new InOperator(field, val)); }
  throw new Error("Not supported query:" + JSON.stringify(wholeQuery));
};

comparisonParserMap.$regex = (field: string, val: any, wholeQuery: any): Operator | false => {
  return new RegexOperator(field, val, wholeQuery.$options);
};

comparisonParserMap.$options = (field: string, val: any, wholeQuery: any): Operator | false => {
  return false;
};

comparisonParserMap.$not = (field: string, val: any, wholeQuery: any): Operator | false => {
  const op = ValueOperator.fromFieldAndQuery(field, val);
  if (op instanceof ValueOperator) { return new NotValueOperator(field, op); }
  throw new Error("Not supported query:" + JSON.stringify(wholeQuery));
};

abstract class Operator {
  static fromMongo(query: any): Operator {
    if (query instanceof Object) {
      const opList: Operator[] = [];
      for (const key of Object.keys(query)) {
        if (key in parserMap) {
          opList.push(parserMap[key](query[key]));
        } else {
          opList.push(ValueOperator.fromFieldAndQuery(key, query[key]));
        }
      }
      return opList.length === 1 ? opList[0] : new AndOperator(opList);
    }
    return FALSE;
  }

  static toMongo(query: Operator): object | undefined {
    if (query === undefined) { return undefined; }
    if (query instanceof FalseOperator) { return undefined; }
    return query.toMongo();
  }

  abstract toMongo(): object;

  toString() { return JSON.stringify(this.toMongo()); }
}

class TrueOperator extends Operator {
  toMongo(): object { return {}; }
}

class FalseOperator extends Operator {
  toMongo(): object { return { bool: false }; }
}

const TRUE = new TrueOperator();
const FALSE = new FalseOperator();

class AndOperator extends Operator {
  constructor(public opList: Operator[]) { super(); }
  toMongo(): object {
    if (this.opList.length === 1) { return this.opList[0].toMongo(); }
    return { $and: this.opList.map((val) => val.toMongo()) };
  }
}

class OrOperator extends Operator {
  constructor(public opList: Operator[]) { super(); }
  toMongo(): object {
    if (this.opList.length === 1) { return this.opList[0].toMongo(); }
    return { $or: this.opList.map((val) => val.toMongo()) };
  }
}

class NotOperator extends Operator {
  constructor(public op: Operator) { super(); }
  toMongo(): object {
    if (this.op instanceof OrOperator) { return { $nor: this.op.opList.map((val) => val.toMongo()) }; }
    return { $not: this.op.toMongo() };
  }
}

abstract class ValueOperator extends Operator {
  constructor(public field: string) { super(); }

  static fromFieldAndQuery(field: string, query: any): Operator {
    if (query instanceof Array) {
      return new EqOperator(field, query);

    } else if (query instanceof Object) {
      const opList: Operator[] = [];
      for (const op of Object.keys(query)) {
        if (op in comparisonParserMap) {
          const _op = comparisonParserMap[op](field, query[op], query);
          if (_op) { opList.push(_op); }
        } else {
          opList.push(new EqOperator(field, { [op]: query[op] }));
        }
      }
      if (opList.length === 1) { return opList[0]; }
      return new AndOperator(opList);
    }
    return new EqOperator(field, query);
  }
}

class NotValueOperator extends ValueOperator {
  constructor(field: string, public op: ValueOperator) { super(field); }
  toMongo(): object {
    if (this.op instanceof EqOperator) { return { [this.field]: { $ne: this.op.val } }; }
    if (this.op instanceof InOperator) { return { [this.field]: { $nin: this.op.val } }; }
    const valueOp = this.op.toMongo() as any;
    const exp: any = valueOp[this.field];
    return { [this.field]: { $not: exp } };
  }
}
class EqOperator extends ValueOperator {
  constructor(field: string, public val: any) { super(field); }
  toMongo(): object { return { [this.field]: this.val }; }
}
class GtOperator extends ValueOperator {
  constructor(field: string, public val: any) { super(field); }
  toMongo(): object { return { [this.field]: { $gt: this.val } }; }
}
class GteOperator extends ValueOperator {
  constructor(field: string, public val: any) { super(field); }
  toMongo(): object { return { [this.field]: { $gte: this.val } }; }
}
class InOperator extends ValueOperator {
  constructor(field: string, public val: any[]) { super(field); }
  toMongo(): object { return { [this.field]: { $in: this.val } }; }
}
class LtOperator extends ValueOperator {
  constructor(field: string, public val: any) { super(field); }
  toMongo(): object { return { [this.field]: { $lt: this.val } }; }
}
class LteOperator extends ValueOperator {
  constructor(field: string, public val: any) { super(field); }
  toMongo(): object { return { [this.field]: { $lte: this.val } }; }
}
class RegexOperator extends ValueOperator {
  constructor(field: string, public val: any, public options?: any) { super(field); }
  toMongo(): object {
    if (this.options) { return { [this.field]: { $regex: this.val, $options: this.options } }; }
    return { [this.field]: { $regex: this.val } };
  }
}

let indent = 0;

const shrinkLogicalQuery = (query: Operator): Operator => {
  if (query instanceof TrueOperator || query instanceof FalseOperator) { return query; }
  indent++;
  try {
    if (showLog) { console.log(" ".repeat(indent) + query); }
    if (query instanceof OrOperator) {
      const intersection = query.opList.reduce(calcIntersection, TRUE);
      if (intersection instanceof AndOperator) {
        const opList = query.opList.map((op) => {
          const andOp = op as AndOperator;
          const newOp = andOp.opList.filter((op) => !intersection.opList.find((iOp) => equal(op, iOp)));
          if (newOp.length === 0) { return TRUE; }
          return new AndOperator(newOp);
        }).reduce((a, b) => {
          if (a instanceof TrueOperator || b instanceof TrueOperator) { return TRUE; }
          a.push(b);
          return a;
        }, [] as Operator[]);
        if (opList instanceof TrueOperator) {
          query = intersection;
        } else {
          query = new AndOperator([...intersection.opList, new OrOperator(opList)]);
        }
      }
    }
    if (query instanceof AndOperator) {
      query.opList = query.opList.map(shrinkLogicalQuery);
    }
    if (query instanceof NotOperator) {
      query = expandNotOperand(shrinkLogicalQuery(query.op));
    }
    return query;
  } finally {
    indent--;
  }
};

const calcIntersection = (a: Operator, b: Operator): Operator => {
  if (a === TRUE && b instanceof AndOperator) { return b; }
  if (a instanceof AndOperator && b instanceof AndOperator) {
    const opList = a.opList.filter((aOp) => b.opList.find((bOp) => equal(aOp, bOp)));
    if (opList.length === 0) { return FALSE; }
    return new AndOperator(opList);
  }
  return FALSE;
};

const expandLogicalQuery = (query: Operator): Operator => {
  if (query instanceof TrueOperator || query instanceof FalseOperator) { return query; }
  indent++;
  try {
    if (showLog) { console.log(" ".repeat(indent) + query); }
    if (query instanceof AndOperator) {
      return query.opList.reduce(expandAndOperand, TRUE);
    }
    if (query instanceof OrOperator) {
      return query.opList.reduce(expandOrOperand, FALSE);
    }
    if (query instanceof NotOperator) {
      return expandNotOperand(query.op);
    }
    return query;
  } finally {
    indent--;
  }
};

const expandAndOperand = (a: Operator, b: Operator): Operator => {
  indent++;
  try {
    if (showLog) { console.log(" ".repeat(indent) + a + " and " + b); }
    b = expandLogicalQuery(b);
    if (a instanceof TrueOperator) { return b; }
    if (b instanceof TrueOperator) { return a; }
    if (a instanceof FalseOperator || b instanceof FalseOperator) { return FALSE; }
    let c: Operator;
    if (a instanceof AndOperator) {
      c = combineLogicalAnd(a, b);
    } else if (a instanceof OrOperator) {
      c = new OrOperator(a.opList.map((x: any) => expandAndOperand(x, b)));
      c = expandLogicalQuery(c);
    } else {
      if (b instanceof AndOperator) {
        c = b.opList.reduce(expandAndOperand, a);
      } else if (b instanceof OrOperator) {
        c = new OrOperator(b.opList.map((x: any) => expandAndOperand(a, x)));
        c = expandLogicalQuery(c);
      } else {
        c = combineLogicalAnd(new AndOperator([a]), b);
      }
    }
    if (showLog) { console.log(" ".repeat(indent) + c); }
    return c;
  } finally {
    indent--;
  }
};

// const isPrimitive = (a: any) => a === null || typeof a === "boolean" || typeof a === "number" || typeof a === "string"
const notAnd = (a: ValueOperator, b: ValueOperator) => a instanceof NotValueOperator && equal(a.op.toMongo(), b.toMongo());
const unequalPrimitive = (a: ValueOperator, b: ValueOperator) => a instanceof EqOperator && b instanceof EqOperator && !equal(a.val, b.val);
const checkContradiction = (a: ValueOperator, b: ValueOperator) => {
  const aGt = a instanceof GtOperator ? a.val : (a instanceof NotValueOperator && a.op instanceof LteOperator ? a.op.val : undefined);
  const aGte = a instanceof GteOperator ? a.val : (a instanceof NotValueOperator && a.op instanceof LtOperator ? a.op.val : undefined);
  const bLt = b instanceof LtOperator ? b.val : (b instanceof NotValueOperator && b.op instanceof GteOperator ? b.op.val : undefined);
  const bLte = b instanceof LteOperator ? b.val : (b instanceof NotValueOperator && b.op instanceof GtOperator ? b.op.val : undefined);
  return (aGt !== undefined && bLt !== undefined && aGt >= bLt) ||
    (aGte !== undefined && bLt !== undefined && aGte >= bLt) ||
    (aGt !== undefined && bLte !== undefined && aGt >= bLte);
};
const containCond = (a: ValueOperator, b: ValueOperator) => {
  const aGt = a instanceof GtOperator ? a.val : undefined;
  const aGte = a instanceof GteOperator ? a.val : undefined;
  const aLt = a instanceof LtOperator ? a.val : undefined;
  const aLte = a instanceof LteOperator ? a.val : undefined;
  const bGt = b instanceof GtOperator ? b.val : undefined;
  const bGte = b instanceof GteOperator ? b.val : undefined;
  const bLt = b instanceof LtOperator ? b.val : undefined;
  const bLte = b instanceof LteOperator ? b.val : undefined;
  return (aGt !== undefined && bGt !== undefined && aGt >= bGt) ||
    (aGte !== undefined && bGt !== undefined && aGte > bGt) ||
    (aGt !== undefined && bGte !== undefined && aGt >= bGte) ||
    (aGte !== undefined && bGte !== undefined && aGte >= bGte) ||
    (aLt !== undefined && bLt !== undefined && aLt <= bLt) ||
    (aLte !== undefined && bLt !== undefined && aLte <= bLt) ||
    (aLt !== undefined && bLte !== undefined && aLt < bLte) ||
    (aLte !== undefined && bLte !== undefined && aLte <= bLte);
};

const andInAndIn = (a: any[], b: any[]): any[] => {
  return a.filter((value, index, self) => b.indexOf(value) >= 0);
};

const arrayDifference = (a: any[], b: any[]): any[] => {
  return a.filter((value, index, self) => b.indexOf(value) < 0);
};

const combineLogicalAnd = (base: AndOperator, addition: Operator): Operator => {
  indent++;
  try {
    if (showLog) { console.log(" ".repeat(indent) + "combineLogicalAnd " + base + " and " + addition); }
    if (addition instanceof ValueOperator) {
      for (let i = 0; i < base.opList.length; i++) {
        const cond = base.opList[i];
        if (cond instanceof ValueOperator && cond.field === addition.field) {
          if (equal(cond.toMongo(), addition.toMongo())) { return base; }
          if (notAnd(cond, addition) || notAnd(addition, cond)) { return FALSE; }
          if (unequalPrimitive(cond, addition)) { return FALSE; }
          if (checkContradiction(cond, addition) || checkContradiction(addition, cond)) { return FALSE; }
          if (containCond(cond, addition)) { return base; }
          if (containCond(addition, cond)) {
            return new AndOperator([...base.opList.slice(0, i), addition, ...base.opList.slice(i + 1)]);
          }
          if (cond instanceof InOperator && addition instanceof EqOperator) {
            const val = andInAndIn(cond.val, [addition.val]);
            if (val.length === 0) { return FALSE; }
            return new AndOperator([...base.opList.slice(0, i), addition, ...base.opList.slice(i + 1)]);
          }
          if (cond instanceof EqOperator && addition instanceof InOperator) {
            const val = andInAndIn([cond.val], addition.val);
            if (val.length === 0) { return FALSE; }
            return base;
          }
          if (cond instanceof InOperator && addition instanceof NotValueOperator && addition.op instanceof InOperator) {
            const val = arrayDifference(cond.val, addition.op.val);
            if (val.length === 0) { return FALSE; }
            return new AndOperator([...base.opList, addition]);
          }
          if (addition instanceof InOperator && cond instanceof NotValueOperator && cond.op instanceof InOperator) {
            const val = arrayDifference(addition.val, cond.op.val);
            if (val.length === 0) { return FALSE; }
            return new AndOperator([...base.opList, addition]);
          }
          if (cond instanceof EqOperator && addition instanceof NotValueOperator && addition.op instanceof InOperator) {
            if (addition.op.val.indexOf(cond.val) >= 0) { return FALSE; }
            return base;
          }
          if (addition instanceof EqOperator && cond instanceof NotValueOperator && cond.op instanceof InOperator) {
            if (cond.op.val.indexOf(addition.val) >= 0) { return FALSE; }
            return new AndOperator([...base.opList.slice(0, i), addition, ...base.opList.slice(i + 1)]);
          }
          if (cond instanceof NotValueOperator && cond.op instanceof InOperator &&
            addition instanceof NotValueOperator && addition.op instanceof InOperator) {
            const val = Array.from(new Set([...cond.op.val, ...addition.op.val]));
            return new AndOperator([
              ...base.opList.slice(0, i),
              new NotValueOperator(cond.field, new InOperator(cond.field, val)),
              ...base.opList.slice(i + 1)]);
          }
        }
      }
    } else if (addition instanceof OrOperator) {
      const query = new OrOperator(addition.opList.map((x: any) => expandAndOperand(base, x)));
      return expandLogicalQuery(query);
    } else if (addition instanceof AndOperator) {
      return addition.opList.reduce(expandAndOperand, base);
    }
    return new AndOperator([...base.opList, addition]);
  } finally {
    indent--;
  }
};

const expandOrOperand = (a: Operator, b: Operator): Operator => {
  indent++;
  try {
    if (showLog) { console.log(" ".repeat(indent) + a + " or " + b); }
    b = expandLogicalQuery(b);
    if (a instanceof FalseOperator) { return b; }
    if (b instanceof FalseOperator) { return a; }
    if (a instanceof TrueOperator || b instanceof TrueOperator) { return TRUE; }
    let c: Operator;
    if (equal(a.toMongo(), b.toMongo())) {
      c = a;
    } else if (a instanceof OrOperator) {
      if (b instanceof OrOperator) {
        c = new OrOperator([...a.opList, ...b.opList]);
      } else {
        c = new OrOperator([...a.opList, b]);
      }
    } else if (b instanceof OrOperator) {
      c = new OrOperator([a, ...b.opList]);
    } else {
      c = new OrOperator([a, b]);
    }
    if (showLog) { console.log(" ".repeat(indent) + c); }
    return c;
  } finally {
    indent--;
  }
};

const expandNotOperand = (a: Operator): Operator => {
  indent++;
  try {
    if (showLog) { console.log(" ".repeat(indent) + "not " + a); }
    if (a instanceof FalseOperator) { return TRUE; }
    if (a instanceof TrueOperator) { return FALSE; }
    if (a instanceof NotValueOperator) { return a.op; }
    if (a instanceof ValueOperator) { return new NotValueOperator(a.field, a); }
    a = expandLogicalQuery(a);
    if (a instanceof FalseOperator) { return TRUE; }
    if (a instanceof TrueOperator) { return FALSE; }
    if (a instanceof NotValueOperator) { return a.op; }
    if (a instanceof ValueOperator) { return new NotValueOperator(a.field, a); }
    let c: Operator;
    if (a instanceof AndOperator) {
      c = new OrOperator(a.opList.map((x: any) => expandNotOperand(x)));
      c = expandLogicalQuery(c);
    } else if (a instanceof OrOperator) {
      c = new AndOperator(a.opList.map((x: any) => expandNotOperand(x)));
      c = expandLogicalQuery(c);
    } else if (a instanceof NotOperator) {
      c = expandLogicalQuery(a.op);
    } else {
      c = new NotOperator(a);
    }
    if (showLog) { console.log(" ".repeat(indent) + c); }
    return c;
  } finally {
    indent--;
  }
};
