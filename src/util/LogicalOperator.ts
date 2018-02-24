import { diff } from "deep-diff"

const $and = "$and"
const $or = "$or"
const $not = "$not"

// TODO 簡易形式からの展開とコンパクト化

export class LogicalOperator {
  static getInsideOfCache(cond: any, cacheCond: any): object | undefined {
    if (cacheCond === undefined) { return cond }
    let query: any = { $and: [cond, cacheCond] }
    indent = 0
    let result = query
    do {
      query = result
      result = expandLogicalQuery(query)
    } while (diff(result, query))
    if (result === false) { return undefined }
    //    if (result === true) { return cond }
    return result
  }

  static getOutsideOfCache(cond: any, cacheCond: any): object | undefined {
    if (cacheCond === undefined) { return undefined }
    let query: any = { $and: [cond, { $not: cacheCond }] }
    indent = 0
    let result = query
    do {
      query = result
      result = expandLogicalQuery(query)
    } while (diff(result, query))
    if (result === false) { return undefined }
    if (result === true) { return cond }
    return result
  }
}

let indent = 0

const expandLogicalQuery = (query: any): any => {
  if (query === true || query === false) { return query }
  indent++;
  try {
    console.log(" ".repeat(indent) + JSON.stringify(query))
    if ($and in query) {
      if (query.$and === false) { return false }
      if (query.$and === true) { return true }
      if (query.$and.length === 0) { return false } // falseが空集合
      return query.$and.reduce(expandAndOperand, true)
    }
    if ($or in query) {
      if (query.$or === false) { return false }
      if (query.$or === true) { return true }
      if (query.$or.length === 0) { return false }
      return query.$or.reduce(expandOrOperand, false)
    }
    if ($not in query) {
      return expandNotOperand(query.$not)
    }
    return query
  } finally {
    indent--;
  }
}

const expandAndOperand = (a: any, b: any) => {
  indent++;
  try {
    console.log(" ".repeat(indent) + JSON.stringify(a) + " and " + JSON.stringify(b))
    b = expandLogicalQuery(b)
    if (a === true) { return b }
    if (b === true) { return a }
    if (a === false || b === false) { return false }
    let c: any = {}
    if ($and in a) {
      if ($and in b) {
        c.$and = combineLogicalAnd(a.$and, b.$and);
      } else if ($or in b) {
        c.$or = b.$or.map((x: any) => expandAndOperand(a, x))
        c = expandLogicalQuery(c)
      } else {
        c.$and = combineLogicalAnd(a.$and, b);
      }
    } else if ($or in a) {
      c.$or = a.$or.map((x: any) => expandAndOperand(x, b))
      c = expandLogicalQuery(c)
    } else {
      if ($and in b) {
        c.$and = combineLogicalAnd([a], b.$and);
      } else if ($or in b) {
        c.$or = b.$or.map((x: any) => expandAndOperand(a, x))
        c = expandLogicalQuery(c)
      } else {
        c.$and = combineLogicalAnd([a], b)
      }
    }
    console.log(" ".repeat(indent) + JSON.stringify(c))
    return c
  } finally {
    indent--;
  }
}

const simplifyVal = (a: any): any => a instanceof Object ? ("$eq" in a ? omitEq(a) : ("$not" in a ? omitNot(a) : a)) : a
const omitNot = (a: any) => a instanceof Object && "$not" in a && a.$not instanceof Object && "$not" in a.$not ? simplifyVal(a.$not.$not) : a
const omitEq = (a: any) => a instanceof Object && "$eq" in a ? simplifyVal(a.$eq) : a
const isPrimitive = (a: any) => a === null || typeof a === "boolean" || typeof a === "number" || typeof a === "string"
const notAnd = (a: any, b: any) => a instanceof Object && $not in a && !diff(omitEq(a.$not), b)
const unequalPrimitive = (a: any, b: any) => isPrimitive(a) && isPrimitive(b) && a !== b
const checkContradiction = (a: any, b: any) => {
  const aGt = a.$gt ? a.$gt : (a.$not instanceof Object && "$lte" in a.$not ? a.$not.$lte : undefined)
  const aGte = a.$gte ? a.$gte : (a.$not instanceof Object && "$lt" in a.$not ? a.$not.$lt : undefined)
  const bLt = b.$lt ? b.$lt : (b.$not instanceof Object && "$gte" in b.$not ? b.$not.$gte : undefined)
  const bLte = b.$lte ? b.$lte : (b.$not instanceof Object && "$gt" in b.$not ? b.$not.$gt : undefined)
  return (aGt !== undefined && bLt !== undefined && aGt >= bLt) ||
    (aGte !== undefined && bLt !== undefined && aGte >= bLt) ||
    (aGt !== undefined && bLte !== undefined && aGt >= bLte)
}

const combineLogicalAnd = (list: any, condA: any) => {
  indent++;
  try {
    console.log(" ".repeat(indent) + "combineLogicalAnd " + JSON.stringify(list) + " and " + JSON.stringify(condA))
    if (list === false) { return false }
    if (list === true) { return [condA] }
    if (($and in condA) || ($or in condA) || ($not in condA)) {
      return list.concat(condA);
    }
    for (const key of Object.keys(condA)) {
      for (const idx of Object.keys(list)) {
        const condB = list[idx]
        if (key in condB) {
          const a = simplifyVal(condA[key])
          const b = simplifyVal(condB[key])
          if (!diff(a, b)) {
            return list;
          }
          if (notAnd(a, b) || notAnd(b, a)) { return false }
          if (unequalPrimitive(a, b)) { return false }
          if (a instanceof Object && b instanceof Object) {
            if (checkContradiction(a, b) || checkContradiction(b, a)) { return false }

          }
        }
      }
    }
    return list.concat(condA);
  } finally {
    indent--;
  }
}

const expandOrOperand = (a: any, b: any) => {
  indent++;
  try {
    console.log(" ".repeat(indent) + JSON.stringify(a) + " or " + JSON.stringify(b))
    b = expandLogicalQuery(b)
    if (a === false) { return b }
    if (b === false) { return a }
    if (a === true || b === true) { return true }
    let c: any = {}
    if ($and in a) {
      if ($and in b) {
        if (!diff(a, b)) {
          c = a
        } else {
          c.$or = [a, b]
        }
      } else if ($or in b) {
        c.$or = [a].concat(b.$or)
      }
    } else if ($or in a) {
      if ($or in b) {
        c.$or = a.$or.concat(b.$or)
      } else {
        c.$or = a.$or.concat(b)
      }
    } else {
      if ($and in b) {
        c.$or = [a, b]
      } else if ($or in b) {
        c.$or = [a].concat(b.$or);
      } else {
        c.$or = [a, b]
      }
    }
    console.log(" ".repeat(indent) + JSON.stringify(c))
    return c
  } finally {
    indent--;
  }
}

const expandNotOperand = (a: any) => {
  indent++;
  try {
    console.log(" ".repeat(indent) + "not " + JSON.stringify(a))
    a = simplifyVal(a)
    if (a === false) { return true }
    if (a === true) { return false }
    if (isPrimitive(a)) { return { $not: a } }
    a = expandLogicalQuery(a)
    a = simplifyVal(a)
    if (a === false) { return true }
    if (a === true) { return false }
    if (isPrimitive(a)) { return { $not: a } }
    let c: any = {}
    if ($and in a) {
      c.$or = a.$and.map((x: any) => expandNotOperand(x))
      c = expandLogicalQuery(c)
    } else if ($or in a) {
      c.$and = a.$or.map((x: any) => expandNotOperand(x))
      c = expandLogicalQuery(c)
    } else if ($not in a) {
      c = expandLogicalQuery(a.$not)
    } else {
      const keys = Object.keys(a)
      if (keys.length === 1) {
        const key = keys[0]
        c[key] = { $not: a[key] }
      } else {
        c.$or = keys.map((key: string) => ({ [key]: expandNotOperand(a[key]) }))
        c = expandLogicalQuery(c)
      }
    }
    console.log(" ".repeat(indent) + JSON.stringify(c))
    return simplifyVal(c)
  } finally {
    indent--;
  }
}
