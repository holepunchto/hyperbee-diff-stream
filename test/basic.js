const test = require('brittle')
const Hyperbee = require('hyperbee')

const getDiffs = require('../index')
const { create, sync } = require('./helpers')

test('no changes -> empty diff', async t => {
  const bases = await setup(t)
  const [base1] = bases

  const diffs = await getDiffs(base1.view.bee, base1.view.bee)
  t.is(diffs.length, 0)
})

test('index moved ahead', async t => {
  const bases = await setup(t)
  const base1 = bases[0]

  const origBee = base1.view.bee.snapshot()

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await base1.append({ entry: ['1-2', '1-entry2'] })

  await confirm(...bases)

  const newBee = base1.view.bee.snapshot()

  t.is(newBee.core.indexedLength, 3) // Sanity check
  const diffs = await getDiffs(origBee, newBee)

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-1', '1-2'])
  t.alike(diffs.map(({ right }) => right), [null, null])
})

test('new bee forked, but no old fork nor changes to index', async t => {
  const bases = await setup(t)
  const base1 = bases[0]

  const origBee = base1.view.bee.snapshot()

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await base1.append({ entry: ['1-2', '1-entry2'] })

  const newBee = base1.view.bee.snapshot()

  t.is(newBee.core.indexedLength, 0) // Sanity check
  const diffs = await getDiffs(origBee, newBee)

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-1', '1-2'])
  t.alike(diffs.map(({ right }) => right), [null, null])
})

test('new continued old fork, but no changes to index', async t => {
  const bases = await setup(t)
  const base1 = bases[0]

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await base1.append({ entry: ['1-2', '1-entry2'] })

  const origBee = base1.view.bee.snapshot()
  const origIndexedL = origBee.core.indexedLength
  t.is(origIndexedL, 0) // Sanity check

  await base1.append({ entry: ['1-3', '1-entry3'] })
  await base1.append({ entry: ['1-4', '1-entry4'] })

  const newBee = base1.view.bee.snapshot()

  const diffs = await getDiffs(origBee, newBee)
  t.is(newBee.feed.indexedLength, 0) // Sanity check

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-3', '1-4'])
  t.alike(diffs.map(({ right }) => right), [null, null])
})

test('both new index and new fork--old had up to date index', async t => {
  const bases = await setup(t)
  const [base1, base2, readOnlyBase] = bases

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await base1.append({ entry: ['1-2', '1-entry2'] })
  await confirm(base1, base2)
  await sync(...bases)

  const origBee = readOnlyBase.view.bee.snapshot()
  const origIndexedL = readOnlyBase.view.bee.core.indexedLength
  t.is(origIndexedL, 3) // Sanity check
  t.is(origBee.version, 3) // Sanity check

  await base1.append({ entry: ['1-3', '1-entry3'] })
  await base1.append({ entry: ['1-4', '1-entry4'] })
  await confirm(base1, base2)

  // Fork
  await base1.append({ entry: ['1-5', '1-entry5'] })
  await sync(base1, readOnlyBase)

  const newBee = readOnlyBase.view.bee.snapshot()

  const diffs = await getDiffs(origBee, newBee)
  t.is(newBee.feed.indexedLength, 5) // Sanity check
  t.is(newBee.version, 6) // Sanity check

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-3', '1-4', '1-5'])
  t.alike(diffs.map(({ right }) => right), [null, null, null])
})

test('new index, new fork and old fork all resolved nicely', async t => {
  const bases = await setup(t)
  const [base1, base2, readOnlyBase] = bases

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await confirm(base1, base2)
  await base1.append({ entry: ['1-2', '1-entry2'] })
  await sync(...bases)

  const origBee = readOnlyBase.view.bee.snapshot()
  const origIndexedL = readOnlyBase.view.bee.core.indexedLength
  t.is(origIndexedL, 2) // Sanity check
  t.is(origBee.version, 3) // Sanity check

  await base1.append({ entry: ['1-3', '1-entry3'] })
  await base1.append({ entry: ['1-4', '1-entry4'] })
  await confirm(base1, base2)

  // New Fork
  await base1.append({ entry: ['1-5', '1-entry5'] })
  await sync(base1, readOnlyBase)

  const newBee = readOnlyBase.view.bee.snapshot()

  const diffs = await getDiffs(origBee, newBee)
  t.is(newBee.feed.indexedLength, 5) // Sanity check
  t.is(newBee.version, 6) // Sanity check

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-3', '1-4', '1-5'])
  t.alike(diffs.map(({ right }) => right), [null, null, null])
})

test('complex autobase linearisation with truncates', async t => {
  const bases = await setup(t)
  const [base1, base2] = bases

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await base1.append({ entry: ['1-2', '1-entry2'] })
  await confirm(base1, base2)

  let hasTruncated = false
  base2.view.bee.core.on('truncate', function () { hasTruncated = true })
  base1.view.bee.core.on('truncate', function () { hasTruncated = true })

  await Promise.all([
    base1.append({ entry: ['1-3', '1-entry3'] }),
    base1.append({ entry: ['1-4', '1-entry4'] }),
    base2.append({ entry: ['2-1', '2-entry1'] }),
    base2.append({ entry: ['2-2', '2-entry2'] }),
    base2.append({ entry: ['2-3', '2-entry3'] })
  ])

  const origBee = base1.view.bee.snapshot()
  const origIndexedL = origBee.core.indexedLength
  t.is(origIndexedL, 3) // Sanity check
  t.is(origBee.version, 5) // Sanity check

  const origBee2 = base2.view.bee.snapshot()
  const origIndexedL2 = origBee2.core.indexedLength
  t.is(origIndexedL2, 3) // Sanity check
  t.is(origBee2.version, 6) // Sanity check

  await confirm(base1, base2)

  const newBee1 = base1.view.bee.snapshot()
  const newBee2 = base2.view.bee.snapshot()

  const diffsBee1 = await getDiffs(origBee, newBee1)
  const diffsBee2 = await getDiffs(origBee2, newBee2)

  t.is(newBee1.feed.indexedLength, 8) // Sanity check
  t.is(newBee1.version, 8) // Sanity check
  t.alike(diffsBee1.map(({ left }) => left.key.toString()), ['2-1', '2-2', '2-3'])
  t.alike(diffsBee1.map(({ right }) => right), [null, null, null])

  t.is(newBee2.feed.indexedLength, 8) // Sanity check
  t.is(newBee2.version, 8) // Sanity check
  t.alike(diffsBee2.map(({ left }) => left.key.toString()), ['1-3', '1-4'])
  t.alike(diffsBee2.map(({ right }) => right), [null, null])

  // Sanity check: we did indeed truncate
  t.is(hasTruncated, true)
})

async function confirm (base1, base2) {
  await sync(base1, base2)
  await base1.append(null)
  await base2.append(null)
  await sync(base1, base2)
  await base1.append(null)
  await base2.append(null)
  await sync(base1, base2)
}

async function setup (t) {
  // 2 writers, 1 read-only
  const bases = await create(3, (...args) => apply(t, ...args), open)
  const [base1, base2] = bases

  await base1.append({
    add: base2.local.key.toString('hex')
  })

  await sync(...bases)
  await base1.append(null)
  await sync(...bases)

  return bases
}

class SimpleView {
  constructor (base, core) {
    this.base = base
    this.bee = new Hyperbee(core, { extension: false, keyEncoding: 'binary', valueEncoding: 'binary' })
  }

  async ready () {
    await this.bee.ready()
  }

  async _applyMessage (key, value) {
    await this.bee.put(key, value, { update: false })
  }
}

function open (linStore, base) {
  const core = linStore.get('simple-bee', { valueEncoding: 'binary' })

  const view = new SimpleView(base, core)
  return view
}

async function apply (t, batch, simpleView, base) {
  for (const { value } of batch) {
    if (value === null) continue
    if (value.add) {
      await base.system.addWriter(Buffer.from(value.add, 'hex'))
    } else {
      try {
        await simpleView._applyMessage(...value.entry)
      } catch (e) {
        console.error(e)
        t.fail()
      }
    }
  }
}
