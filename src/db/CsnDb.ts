
export interface CsnDb {

  start(): Promise<void>

  /**
   * increment csn
   */
  increment(): Promise<number>

  /**
   * Obtain current CSN
   */
  getCurrentCsn(): Promise<number>

  update(seq: number): Promise<void>
}