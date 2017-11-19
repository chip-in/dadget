import * as hash from "object-hash"

export const enum TransactionType {
  INSERT = "insert",
  UPDATE = "update",
  DELETE = "delete"
}

/**
 * トランザクション要求
 */
export class TransactionRequest {

  /**
   * "insert", "update", "delete" のいずれか
   */
  type: TransactionType

  /**
   * RFC4122 準拠のUUIDであり、オブジェクトの_id属性の値である。
   */
  target: string

  /**
   * insertのときのみ
   * 
   * 追加するオブジェクト。中身の形式は自由であるが、chip-in で予約されている _id, csn の2つの属性を含んでいてはいけない
   */
  new?: object

  /**
   * update,delete のとき
   * 
   * 更新/削除するオブジェクトの直前の値（前提となるコンテキスト通番での値）で、_id, csn の2つの属性を含んでいなければならない
   */
  before?: { [key: string]: any }

  /**
   * updateのときのみ
   * 
   * 更新内容を記述するオペレータ。意味はmongoDB に準ずる。
   */
  operator?: { [op: string]: any }

  /**
   * 更新operator適用
   * @param transaction 
   */
  static applyOperator(transaction: TransactionRequest): { [key: string]: any } {
    if (!transaction.before) throw new Error("transaction.before is missing.")
    if (!transaction.operator) throw new Error("transaction.operator is missing.")
    let obj = Object.assign({}, transaction.before) as { [key: string]: any }
    let operator = transaction.operator
    for (let op in operator) {
      switch (op) {
        // TODO $currentDate の場合の処理。内部的に日時設定
        case "$inc":
          {
            let list = operator[op] as { [key: string]: number }
            for (let key in list) {
              obj = apply$inc(obj, key.split("."), list[key])
            }
          }
          break
        case "$set":
          {
            let list = operator[op] as { [key: string]: any }
            for (let key in list) {
              obj = apply$set(obj, key.split("."), list[key])
            }
          }
          break

      }

    }
    return obj
  }
}

function apply$inc(obj: { [key: string]: any }, keys: string[], inc: number): { [key: string]: any } {
  let key = keys.shift()
  if (key == null) throw new Error()
  if (keys.length == 0) {
    if (Array.isArray(obj)) {
      let pos = Number(key)
      return [...obj.slice(0, pos), (obj[pos] || 0) + inc, ...obj.slice(pos + 1)]
    } else {
      return { ...obj, [key]: (obj[key] || 0) + inc }
    }
  } else {
    if (Array.isArray(obj)) {
      let pos = Number(key)
      return [...obj.slice(0, pos), apply$inc(obj[pos], keys, inc), ...obj.slice(pos + 1)]
    } else {
      return { ...obj, [key]: apply$inc(obj[key], keys, inc) }
    }
  }
}

function apply$set(obj: { [key: string]: any }, keys: string[], val: any): { [key: string]: any } {
  let key = keys.shift()
  if (key == null) throw new Error()
  if (keys.length == 0) {
    if (Array.isArray(obj)) {
      let pos = Number(key)
      return [...obj.slice(0, pos), val, ...obj.slice(pos + 1)]
    } else {
      return { ...obj, [key]: val}
    }
  } else {
    if (Array.isArray(obj)) {
      let pos = Number(key)
      return [...obj.slice(0, pos), apply$set(obj[pos], keys, val), ...obj.slice(pos + 1)]
    } else {
      return { ...obj, [key]: apply$set(obj[key], keys, val) }
    }
  }
}

/**
 * トランザクションオブジェクト
 */
export class TransactionObject extends TransactionRequest {

  /**
   * トランザクションをコミットしたコンテキスト通番
   */
  csn: number

  /**
   * 	トランザクションをコミットした時刻
   */
  datetime: number

  /**
   * 1個前のトランザクションオブジェクトのdigestの値
   */
  beforeDigest: string

  /**
   * トランザクションオブジェクトからこの属性を除いたものを object-hash により、{ algorithm: "md5", encoding: "base64" }ハッシュした値
   */
  digest?: string

  /**
   * トランザクションハッシュ生成
   * @param transaction 
   */
  static calcDigest(transaction: TransactionObject) {
    return hash(transaction, { algorithm: "md5", encoding: "base64" })
  }
}