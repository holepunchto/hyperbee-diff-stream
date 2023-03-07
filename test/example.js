const { execFile } = require('node:child_process')
const util = require('node:util')
const test = require('brittle')

test.solo('example works', async (t) => {
  const res = await util.promisify(execFile)('node', ['./example.js'])

  const expected = `- Updated e1 from entry1 to Something-else
- Removed e2
- Set e3 to entry3
`

  t.is(res.stdout, expected)
  t.is(res.error, undefined)
})
