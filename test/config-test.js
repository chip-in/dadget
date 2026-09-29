var chai = require('chai')
var assert = chai.assert

// MAX_ROWS_NUM は CNODE_WSOCKET_MAX_PAYLOAD(WebSocket中継の1メッセージ上限)から導出される。
// 既定値(104857600 = 100MiB)で 2,000,000行 となる比率で、無効値は既定値にフォールバックする。
const configPath = require.resolve('../lib/Config')

function loadMaxRowsNum(envValue) {
  const saved = process.env.CNODE_WSOCKET_MAX_PAYLOAD
  if (envValue === undefined) {
    delete process.env.CNODE_WSOCKET_MAX_PAYLOAD
  } else {
    process.env.CNODE_WSOCKET_MAX_PAYLOAD = envValue
  }
  delete require.cache[configPath]
  try {
    return require(configPath).MAX_ROWS_NUM
  } finally {
    if (saved === undefined) {
      delete process.env.CNODE_WSOCKET_MAX_PAYLOAD
    } else {
      process.env.CNODE_WSOCKET_MAX_PAYLOAD = saved
    }
    delete require.cache[configPath]
    require(configPath)
  }
}

describe('Config MAX_ROWS_NUM', function () {
  const cases = [
    { env: undefined, expected: 2000000, name: '未設定 → 既定の2,000,000' },
    { env: '104857600', expected: 2000000, name: '100MiB → 2,000,000' },
    { env: '209715200', expected: 4000000, name: '200MiB → 4,000,000 (比例)' },
    { env: '52428800', expected: 1000000, name: '50MiB → 1,000,000 (比例)' },
    { env: '157286400', expected: 3000000, name: '150MiB → 3,000,000' },
    { env: '209715201', expected: 4000000, name: '200MiB+1 → 上限4,000,000で頭打ち' },
    { env: '314572800', expected: 4000000, name: '300MiB → 上限4,000,000で頭打ち(6,000,000にはならない)' },
    { env: '629145600', expected: 4000000, name: '600MiB → 上限4,000,000で頭打ち' },
    { env: 'abc', expected: 2000000, name: '数値でない → 既定値' },
    { env: '0', expected: 2000000, name: '0 → 既定値' },
    { env: '-5', expected: 2000000, name: '負数 → 既定値' },
    { env: '', expected: 2000000, name: '空文字 → 既定値' },
  ]
  for (const c of cases) {
    it(c.name, function () {
      assert.equal(loadMaxRowsNum(c.env), c.expected)
    })
  }

  it('端数は切り捨てられる', function () {
    // 100MiB+1バイト → 2,000,000.00002... → 2,000,000
    assert.equal(loadMaxRowsNum('104857601'), 2000000)
  })
})
