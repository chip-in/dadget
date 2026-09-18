var chai = require('chai')
var EventEmitter = require('events')
var { SubsetStorage } = require('../lib/se/SubsetStorage')
var { EXPORT_LIMIT_NUM, MAX_EXPORT_NUM } = require('../lib/Config')
var EJSON = require('../lib/util/Ejson')
var assert = chai.assert

// テスト対象: procQuery(SubsetStorage.onReceive内)の2段階取得
//   - 通常: phase1({_id:1})で件数判定 → phase2でフィルタ再実行
//   - phase1が遅い(ID_FETCH_THRESHOLD_MS=500ms超)場合はqueryByIdsで_id取得に切替
//   - 0件は短絡、export(limit=EXPORT_LIMIT_NUM)はphase1省略
// SubsetStorage.query をスタブして、Mongoやコアノード無しで判定ロジックだけを検証する

const SLOW = 600 // ID_FETCH_THRESHOLD_MS(500ms) を超える遅延
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const isIdOnly = (projection) => projection && Object.keys(projection).length === 1 && projection._id === 1
const isByIdQuery = (query) => query && query._id && Array.isArray(query._id.$in)

// handlers: { phase1: (call) => result, byId: (call) => result, filter: (call) => result }
function makeStorage(handlers) {
  const storage = new SubsetStorage({ database: 'testdb', subset: 'wholeContents', type: 'persistent' })
  const calls = []
  storage.query = async function (csn, query, sort, limit, csnMode, projection, offset) {
    const call = { csn, query, sort, limit, csnMode, projection, offset }
    calls.push(call)
    if (isByIdQuery(query)) { return handlers.byId(call) }
    if (isIdOnly(projection)) { return handlers.phase1(call) }
    return handlers.filter(call)
  }
  return { storage, calls }
}

function post(storage, request) {
  return new Promise((resolve, reject) => {
    const req = new EventEmitter()
    req.url = '/d/testdb/subset/wholeContents/query/_get'
    req.method = 'POST'
    req.headers = {}
    const chunks = []
    const res = {
      writeHead() { },
      write(data) { chunks.push(data) },
      end() {
        try { resolve(EJSON.parse(chunks.join(''))) } catch (e) { reject(e) }
      },
    }
    storage.onReceive(req, res).catch(reject)
    process.nextTick(() => {
      req.emit('data', EJSON.stringify(request))
      req.emit('end')
    })
  })
}

function request(query, opts) {
  opts = opts || {}
  return {
    csn: opts.csn || 0,
    query: EJSON.stringify(query),
    sort: opts.sort ? EJSON.stringify(opts.sort) : undefined,
    limit: opts.limit,
    offset: opts.offset,
    csnMode: opts.csnMode,
    projection: opts.projection ? EJSON.stringify(opts.projection) : undefined,
    version: 1,
  }
}

function ids(n, prefix) {
  const list = []
  for (let i = 0; i < n; i++) { list.push({ _id: (prefix || 'id') + i }) }
  return list
}

describe('SubsetStorage procQuery (2段階取得)', function () {
  this.timeout(10000)
  const filter = { juumin_status: { $in: ['1', '2', '', null] } }

  it('phase1が速ければphase2でフィルタを再実行する', async function () {
    const { storage, calls } = makeStorage({
      phase1: () => ({ csn: 10, resultSet: ids(3), restQuery: undefined }),
      filter: (call) => ({ csn: 10, resultSet: [{ _id: 'id0', v: 1 }, { _id: 'id1', v: 2 }, { _id: 'id2', v: 3 }], restQuery: undefined }),
      byId: () => { throw new Error('by-id must not be used') },
    })
    const res = await post(storage, request(filter, { projection: { v: 1 } }))
    assert.equal(res.status, 'OK')
    assert.equal(res.result.resultSet.length, 3)
    assert.equal(calls.length, 2)
    assert.deepEqual(calls[0].projection, { _id: 1 })
    assert.deepEqual(calls[1].query, filter)
    assert.deepEqual(calls[1].projection, { v: 1 })
  })

  it('phase1が0件ならフィルタを再実行せず、phase1のcsnで空を返す', async function () {
    const { storage, calls } = makeStorage({
      phase1: () => ({ csn: 42, resultSet: [], restQuery: undefined }),
      filter: () => { throw new Error('filter must not be re-run') },
      byId: () => { throw new Error('by-id must not be used') },
    })
    const res = await post(storage, request(filter))
    assert.equal(res.status, 'OK')
    assert.deepEqual(res.result.resultSet, [])
    assert.equal(res.result.csn, 42)
    assert.equal(calls.length, 1)
  })

  it('phase1がMAX_EXPORT_NUM*10件を超えたらHUGEでidリストを返す', async function () {
    const n = MAX_EXPORT_NUM * 10 + 1
    const { storage, calls } = makeStorage({
      phase1: () => ({ csn: 5, resultSet: ids(n), restQuery: undefined }),
      filter: () => { throw new Error('filter must not be re-run') },
      byId: () => { throw new Error('by-id must not be used') },
    })
    const res = await post(storage, request(filter))
    assert.equal(res.status, 'HUGE')
    assert.equal(res.result.resultSet.length, n)
    assert.equal(calls.length, 1)
  })

  it('phase1が遅ければ_id取得に切り替え、csn固定・phase1の順序・restQueryを維持する', async function () {
    const order = ['c', 'a', 'b']
    const { storage, calls } = makeStorage({
      phase1: async () => {
        await sleep(SLOW)
        return { csn: 77, resultSet: order.map((id) => ({ _id: id })), restQuery: { rest: 1 } }
      },
      filter: () => { throw new Error('filter must not be re-run') },
      // わざと別順序で返す → phase1の順序に並べ直されること
      byId: (call) => ({ csn: 77, resultSet: [{ _id: 'a', v: 'A' }, { _id: 'b', v: 'B' }, { _id: 'c', v: 'C' }], restQuery: undefined }),
    })
    const res = await post(storage, request(filter, { projection: { v: 1 }, sort: { seq: 1 }, limit: 10 }))
    assert.equal(res.status, 'OK')
    assert.deepEqual(res.result.resultSet.map((r) => r._id), order)
    assert.deepEqual(res.result.restQuery, { rest: 1 })
    assert.equal(res.result.csn, 77)
    assert.equal(calls.length, 2)
    const byId = calls[1]
    assert.deepEqual(byId.query, { _id: { $in: order } })
    assert.equal(byId.csn, 77, 'csnはphase1に固定される')
    assert.isUndefined(byId.csnMode, 'csnModeは未指定(latestへ繰り上げない)')
    assert.deepEqual(byId.projection, { v: 1 })
    assert.isUndefined(byId.sort, 'sortは再適用しない')
    assert.isUndefined(byId.offset, 'offsetは再適用しない')
  })

  it('_id取得はMAX_EXPORT_NUM件ずつに分割され、全件がphase1の順序で返る', async function () {
    const n = MAX_EXPORT_NUM * 2 + 500
    const { storage, calls } = makeStorage({
      phase1: async () => { await sleep(SLOW); return { csn: 1, resultSet: ids(n), restQuery: undefined } },
      filter: () => { throw new Error('filter must not be re-run') },
      byId: (call) => ({ csn: 1, resultSet: call.query._id.$in.map((id) => ({ _id: id, v: id })), restQuery: undefined }),
    })
    const res = await post(storage, request(filter))
    const byIdCalls = calls.filter((c) => isByIdQuery(c.query))
    assert.deepEqual(byIdCalls.map((c) => c.query._id.$in.length), [MAX_EXPORT_NUM, MAX_EXPORT_NUM, 500])
    assert.equal(res.result.resultSet.length, n)
    assert.deepEqual(res.result.resultSet.map((r) => r._id), ids(n).map((r) => r._id))
  })

  it('_id取得で見つからなかった_idは結果から除外される', async function () {
    const { storage } = makeStorage({
      phase1: async () => { await sleep(SLOW); return { csn: 1, resultSet: ids(3), restQuery: undefined } },
      filter: () => { throw new Error('filter must not be re-run') },
      byId: () => ({ csn: 1, resultSet: [{ _id: 'id0' }, { _id: 'id2' }], restQuery: undefined }),
    })
    const res = await post(storage, request(filter))
    assert.deepEqual(res.result.resultSet.map((r) => r._id), ['id0', 'id2'])
  })

  it('_idを除外する射影({_id:0})では_id取得を使わずフィルタを再実行する', async function () {
    const { storage, calls } = makeStorage({
      phase1: async () => { await sleep(SLOW); return { csn: 1, resultSet: ids(2), restQuery: undefined } },
      filter: () => ({ csn: 1, resultSet: [{ v: 1 }, { v: 2 }], restQuery: undefined }),
      byId: () => { throw new Error('by-id must not be used') },
    })
    const res = await post(storage, request(filter, { projection: { _id: 0, v: 1 } }))
    assert.equal(res.status, 'OK')
    assert.equal(calls.length, 2)
    assert.deepEqual(calls[1].query, filter)
  })

  it('_id取得が失敗したらフィルタ再実行にフォールバックする', async function () {
    const { storage, calls } = makeStorage({
      phase1: async () => { await sleep(SLOW); return { csn: 1, resultSet: ids(2), restQuery: undefined } },
      filter: () => ({ csn: 1, resultSet: [{ _id: 'id0' }, { _id: 'id1' }], restQuery: undefined }),
      byId: () => { throw new Error('E2402 simulated') },
    })
    const res = await post(storage, request(filter))
    assert.equal(res.status, 'OK')
    assert.equal(res.result.resultSet.length, 2)
    assert.isTrue(calls.some((c) => isByIdQuery(c.query)), '_id取得を試みる')
    assert.deepEqual(calls[calls.length - 1].query, filter, '最後にフィルタで再実行する')
  })

  it('export(limit=EXPORT_LIMIT_NUM)はphase1を省略して直接取得する', async function () {
    const { storage, calls } = makeStorage({
      phase1: () => { throw new Error('phase1 must be skipped for export') },
      filter: (call) => ({ csn: 1, resultSet: [{ _id: 'id0', v: 1 }], restQuery: undefined }),
      byId: () => { throw new Error('by-id must not be used') },
    })
    const res = await post(storage, request(filter, { limit: EXPORT_LIMIT_NUM, projection: { v: 1 } }))
    assert.equal(res.status, 'OK')
    assert.equal(calls.length, 1)
    assert.deepEqual(calls[0].projection, { v: 1 })
    assert.equal(calls[0].limit, EXPORT_LIMIT_NUM)
  })
})

describe('SubsetStorage queryByIds', function () {
  it('idResultのcsn・restQueryを引き継ぎ、resultSetだけを差し替える', async function () {
    const { storage } = makeStorage({
      phase1: () => { throw new Error('unused') },
      filter: () => { throw new Error('unused') },
      byId: (call) => ({ csn: 9, resultSet: call.query._id.$in.map((id) => ({ _id: id, v: id.toUpperCase() })), restQuery: { bogus: 1 } }),
    })
    const idResult = { csn: 9, resultSet: [{ _id: 'x' }, { _id: 'y' }], restQuery: { keep: true }, csnMode: 'latest' }
    const result = await storage.queryByIds(idResult, { v: 1 })
    assert.equal(result.csn, 9)
    assert.deepEqual(result.restQuery, { keep: true })
    assert.equal(result.csnMode, 'latest')
    assert.deepEqual(result.resultSet, [{ _id: 'x', v: 'X' }, { _id: 'y', v: 'Y' }])
  })
})
