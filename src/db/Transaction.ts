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
  before?: object

  /**
   * updateのときのみ
   * 
   * 更新内容を記述するオペレータ。意味はmongoDB に準ずる。
   */
  operator?: object
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
    let jsonStr = JSON.stringify(transaction)
    console.log(jsonStr)
    return hash(transaction, { algorithm: "md5", encoding: "base64" })
  }
}