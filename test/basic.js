const test = require('brittle')
const Hyperbee = require('hyperbee')
const b4a = require('b4a')
const SubEncoder = require('sub-encoder')
const ram = require('random-access-memory')
const Hypercore = require('hypercore')

const BeeDiffStream = require('../index')
const { create, sync } = require('./helpers')

test('no changes -> empty diff', async t => {
  const bases = await setup(t)
  const [base1] = bases

  const diffs = await streamToArray(new BeeDiffStream(base1.view.bee, base1.view.bee))
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
  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee))

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
  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee))

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

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee))
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

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee))
  t.is(newBee.feed.indexedLength, 5) // Sanity check
  t.is(newBee.version, 6) // Sanity check

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-3', '1-4', '1-5'])
  t.alike(diffs.map(({ right }) => right), [null, null, null])
})

test('local version > 0, indexedLength still 0--merge in remote fork', async t => {
  const bases = await setup(t)
  const [base1, base2, readOnlyBase] = bases

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await base1.append({ entry: ['1-2', '1-entry2'] })
  await base2.append({ entry: ['2-1', '2-entry1'] })

  const origBee = base2.view.bee.snapshot()
  const origIndexedL = base2.view.bee.core.indexedLength
  t.is(origIndexedL, 0) // Sanity check
  t.is(origBee.version, 2) // Sanity check

  await sync(...bases)

  const newBee = readOnlyBase.view.bee.snapshot()

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee))
  t.is(newBee.feed.indexedLength, 0) // Sanity check
  t.is(newBee.version, 4) // Sanity check

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-1', '1-2'])
  t.alike(diffs.map(({ right }) => right), [null, null])
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

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee))
  t.is(newBee.feed.indexedLength, 5) // Sanity check
  t.is(newBee.version, 6) // Sanity check

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-3', '1-4', '1-5'])
  t.alike(diffs.map(({ right }) => right), [null, null, null])
})

test('new index, new fork and old fork all resolved nicely (deletes)', async t => {
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

  await base1.append({ delete: '1-1' })
  await base1.append({ entry: ['1-3', '1-entry3'] })
  await base1.append({ entry: ['1-4', '1-entry4'] })
  await confirm(base1, base2)

  // New Fork
  await base1.append({ entry: ['1-5', '1-entry5'] })
  await base1.append({ delete: '1-3' })
  await sync(base1, readOnlyBase)

  const newBee = readOnlyBase.view.bee.snapshot()

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee))
  t.is(newBee.feed.indexedLength, 6) // Sanity check
  t.is(newBee.version, 8) // Sanity check

  t.alike(diffs.map(({ left }) => left?.key.toString()), [undefined, '1-4', '1-5'])
  t.alike(diffs.map(({ right }) => right?.key.toString()), ['1-1', undefined, undefined])
})

test('new snapshot has same final value as old fork but through different path ->no change', async t => {
  const bases = await setup(t, { openFun: encodedOpen })
  const [base1, base2] = bases

  await base1.append({ entry: ['shared', 'shared-entry'] })
  await base1.append({ entry: ['to-be-deleted', 'shared-delete'] })

  await confirm(base1, base2)
  // Both bases will modify shared to 'change' and will delete 'to-be-deleted'
  // but through a different series of operations

  await base1.append({ entry: ['shared', 'I'] })
  await base1.append({ entry: ['shared', 'like'] })
  await base1.append({ entry: ['shared', 'local'] })
  await base1.append({ entry: ['shared', 'change'] })
  await base1.append({ entry: ['to-be-deleted', 'about to be deleted'] })
  await base1.append({ delete: 'to-be-deleted' })

  const origBee = base1.view.bee.snapshot()
  // Normally base1 would now create the diffStream and yield the changes to this point
  // So reaching here, it has yielded 'change' and the deletion already

  // Now base2 also makes local changes to the same entries
  // ending up with the same net changes
  await base2.append({ entry: ['shared', 'Different path'] })
  await base2.append({ entry: ['shared', 'But same resulting'] })
  await base2.append({ entry: ['shared', 'change'] })
  await base2.append({ entry: ['something', 'else'] })
  await base2.append({ delete: 'to-be-deleted' })

  await confirm(base1, base2)
  const newBee = base1.view.bee.snapshot() // Need only yield 'something->else' as change

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee))
  t.alike(diffs, [{ left: { seq: 12, key: 'something', value: 'else' }, right: null }])
})

test('both old and new made changes to the same key -> new value yielded, but source = the old value', async t => {
  const bases = await setup(t, { openFun: encodedOpen })
  const [base1, base2] = bases

  await base1.append({ entry: ['shared', 'shared-entry'] })

  await confirm(base1, base2)
  // Both bases will modify 'shared''

  await base1.append({ entry: ['shared', 'I'] })
  await base1.append({ entry: ['shared', 'modify'] })

  const origBee = base1.view.bee.snapshot()
  // Normally base1 would now create the diffStream and yield the changes to this point
  // So reaching here, it has yielded 'modify' as current state

  // Now base2 also makes local changes to the same entry
  // ending up with a different value
  await base2.append({ entry: ['shared', 'Different path'] })
  await base2.append({ entry: ['shared', 'Different result'] })

  // The linearisation alg will make base2 win
  await confirm(base1, base2)
  const newBee = base1.view.bee.snapshot()
  t.is((await newBee.get('shared')).value, 'Different result') // Sanity check on linearisation order

  // the change to yield now is from base1's last value -> the current value
  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee))
  const sourceEntry = { seq: 3, key: 'shared', value: 'modify' }
  const destEntry = { seq: 5, key: 'shared', value: 'Different result' }
  t.alike(diffs, [{ left: destEntry, right: sourceEntry }])
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

  const diffsBee1 = await streamToArray(new BeeDiffStream(origBee, newBee1))
  const diffsBee2 = await streamToArray(new BeeDiffStream(origBee2, newBee2))

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

test('complex autobase linearisation with truncates and deletes', async t => {
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
    base1.append({ delete: '1-1' }),
    base2.append({ entry: ['2-1', '2-entry1'] }),
    base2.append({ entry: ['2-2', '2-entry2'] })
  ])

  await base1.append({ delete: '1-3' })

  const origBee = base1.view.bee.snapshot()
  const origIndexedL = origBee.core.indexedLength
  t.is(origIndexedL, 3) // Sanity check
  t.is(origBee.version, 7) // Sanity check

  const origBee2 = base2.view.bee.snapshot()
  const origIndexedL2 = origBee2.core.indexedLength
  t.is(origIndexedL2, 3) // Sanity check
  t.is(origBee2.version, 5) // Sanity check

  await confirm(base1, base2)

  const newBee1 = base1.view.bee.snapshot()
  const newBee2 = base2.view.bee.snapshot()

  const diffsBee1 = await streamToArray(new BeeDiffStream(origBee, newBee1))
  const diffsBee2 = await streamToArray(new BeeDiffStream(origBee2, newBee2))

  t.is(newBee1.feed.indexedLength, 9) // Sanity check
  t.is(newBee1.version, 9) // Sanity check
  t.alike(diffsBee1.map(({ left }) => left.key.toString()), ['2-1', '2-2'])
  t.alike(diffsBee1.map(({ right }) => right), [null, null])

  // TODO: this test sometimes fails (non-deterministically) with
  // indexedLength of newBee2 only 8 instead of 9
  // console.log('bee1: ', newBee1.feed.indexedLength, 'bee2 indexedL: ', newBee2.feed.indexedLength)
  t.is(newBee2.feed.indexedLength, 9) // Sanity check
  t.is(newBee2.version, 9) // Sanity check
  t.alike(diffsBee2.map(({ left }) => left?.key.toString()), [undefined, '1-4'])
  t.alike(diffsBee2.map(({ right }) => right?.key.toString()), ['1-1', undefined]) // deletions

  // Sanity check: we did indeed truncate
  t.is(hasTruncated, true)
})

test('works with normal hyperbee', async function (t) {
  const bee = new Hyperbee(new Hypercore(ram))
  await bee.put('e1', 'entry1')

  const oldSnap = bee.snapshot()

  await bee.put('e2', 'entry2')
  await bee.put('e3', 'entry3')
  await bee.del('e2')
  await bee.del('e1')

  const newSnap = bee.snapshot()
  const diffs = await streamToArray(new BeeDiffStream(oldSnap, newSnap))

  t.alike(diffs.map(({ left }) => left?.key.toString()), [undefined, 'e3'])
  t.alike(diffs.map(({ right }) => right?.key.toString()), ['e1', undefined]) // deletions

  const directDiffs = await streamToArray(newSnap.createDiffStream(oldSnap.version))
  t.alike(directDiffs, diffs)
})

test('can handle hyperbee without key or value encoding', async function (t) {
  const bases = await setup(t)

  const base1 = bases[0]
  const bee = base1.view.bee
  bee.keyEncoding = null
  bee.valueEncoding = null

  const oldBee = bee.snapshot()
  await base1.append({ entry: ['1-1', '1-entry1'] })

  const diffs = await streamToArray(new BeeDiffStream(oldBee, bee.snapshot()))
  t.alike(diffs.map(({ left }) => left?.key), [b4a.from('1-1')])
  t.alike(diffs.map(({ right }) => right?.key), [undefined]) // deletions
})

test('yields with original encoding', async function (t) {
  const bases = await setup(t, { openFun: encodedOpen })

  const [base1, base2] = bases
  const bee = base1.view.bee

  await base1.append({ entry: ['1-1', { name: 'name1' }] })
  const oldBee = bee.snapshot()
  await confirm(base1, base2)

  await base2.append({ entry: ['2-1', { name: '2-name1' }] })
  await base1.append({ entry: ['1-2', { name: 'name2' }] })
  await base1.append({ delete: '1-1' })

  await confirm(base1, base2)

  const diff = await streamToArray(new BeeDiffStream(oldBee, bee.snapshot()))
  const expected = [
    {
      left: null,
      right: {
        seq: 1,
        key: '1-1',
        value: { name: 'name1' }
      }
    },
    {
      left: {
        seq: 2,
        key: '1-2',
        value: { name: 'name2' }
      },
      right: null
    }, {
      left: {
        seq: 4,
        key: '2-1',
        value: { name: '2-name1' }
      },
      right: null
    }
  ]
  t.alike(diff, expected)
})

test('can pass diffStream range opts', async function (t) {
  const bases = await setup(t, { openFun: encodedOpen })

  const [base1, base2] = bases
  const bee = base1.view.bee

  await base1.append({ entry: ['1-1', { name: 'name1' }] })
  const oldBee = bee.snapshot()
  await confirm(base1, base2)

  await base2.append({ entry: ['2-1', { name: '2-name1' }] })
  await base1.append({ entry: ['1-2', { name: 'name2' }] })
  await base1.append({ delete: '1-1' })

  await confirm(base1, base2)

  const diff = await streamToArray(new BeeDiffStream(oldBee, bee.snapshot(), {
    gt: '1-1',
    lt: '2-1'
  }))
  const expected = [
    {
      left: {
        seq: 2,
        key: '1-2',
        value: { name: 'name2' }
      },
      right: null
    }
  ]
  t.alike(diff, expected)
})

test('diffStream range opts are encoded (handles sub-encodings)', async function (t) {
  const bases = await setup(t)

  const [base1, base2] = bases
  const bee = base1.view.bee

  await base1.append({ entry: ['not-subbed', 'no'] }) // Before the sub, to check it is not included

  // hack to use a sub-encoding from now on
  const enc = new SubEncoder()
  bee.keyEncoding = enc.sub('sub')

  await base1.append({ entry: ['a-before', 'entry1'] })

  // sanity check that the 'not-subbed' entry is indeed not in the sub
  t.alike(
    (await bee.get('not-subbed', { keyEncoding: 'binary' })).key,
    b4a.from('not-subbed')
  )
  t.is(await bee.get('not-subbed'), null)

  // Add more subbed entries
  const oldBee = bee.snapshot()
  await confirm(base1, base2)

  await base2.append({ entry: ['z-after', '2-entry1'] })
  await base1.append({ entry: ['included', 'entry2'] })
  await base1.append({ delete: 'a-before' })

  await confirm(base1, base2)

  // Diff stream should apply the 'gt' and 'st' conditions only to the sub
  // so 'not-subbed' will not be included, even though it fits in the range
  const diff = await streamToArray(new BeeDiffStream(oldBee, bee.snapshot(), {
    gt: 'a-before',
    lt: 'z-after'
  }))
  const expected = [
    {
      left: {
        seq: 3,
        key: b4a.from('included'),
        value: b4a.from('entry2')
      },
      right: null
    }
  ]
  t.alike(diff, expected)
})

test('can pass in key- or valueEncoding', async function (t) {
  const bases = await setup(t)
  const base1 = bases[0]

  const bee = base1.view.bee
  const origBee = bee.snapshot()

  await base1.append({ entry: ['1-1', '1-entry1'] })

  t.alike((await bee.get('1-1')).key, b4a.from('1-1')) // Sanity check that encoding is binary
  const keyTextDiffs = await streamToArray(new BeeDiffStream(origBee, bee.snapshot(), { keyEncoding: 'utf-8' }))
  const valueTextDiffs = await streamToArray(new BeeDiffStream(origBee, bee.snapshot(), { valueEncoding: 'utf-8' }))

  t.alike(keyTextDiffs, [{ left: { seq: 1, key: '1-1', value: b4a.from('1-entry1') }, right: null }])
  t.alike(valueTextDiffs, [{ left: { seq: 1, key: b4a.from('1-1'), value: '1-entry1' }, right: null }])
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

async function setup (t, { openFun = open } = {}) {
  // 2 writers, 1 read-only
  const bases = await create(3, (...args) => apply(t, ...args), openFun)
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
  constructor (base, core, opts = {}) {
    this.base = base
    this.bee = new Hyperbee(core, { extension: false, keyEncoding: 'binary', valueEncoding: 'binary', ...opts })
  }

  async ready () {
    await this.bee.ready()
  }

  async _applyMessage (key, value) {
    await this.bee.put(key, value, { update: false })
  }

  async getMessage (key) {
    return await this.bee.get(key, { update: false })
  }
}

function open (linStore, base) {
  const core = linStore.get('simple-bee')

  const view = new SimpleView(base, core)
  return view
}

function encodedOpen (linStore, base) {
  return new SimpleView(base, linStore.get('simple-bee'), {
    keyEncoding: 'utf-8',
    valueEncoding: 'json'
  })
}

async function apply (t, batch, simpleView, base) {
  for (const { value } of batch) {
    if (value === null) continue
    if (value.add) {
      await base.system.addWriter(Buffer.from(value.add, 'hex'))
    } else {
      try {
        if (value.delete) {
          await simpleView.bee.del(value.delete, { update: false })
        } else if (value.entry) {
          await simpleView._applyMessage(...value.entry)
        } else {
          throw new Error('unexpected value:', value)
        }
      } catch (e) {
        console.error(e)
        t.fail()
      }
    }
  }
}

async function streamToArray (stream) {
  const res = []
  for await (const entry of stream) {
    res.push(entry)
  }
  return res
}
