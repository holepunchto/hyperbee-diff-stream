const test = require('brittle')
const Hyperbee = require('hyperbee')
const b4a = require('b4a')
const Hypercore = require('hypercore')
const tmpDir = require('test-tmp')
const Corestore = require('corestore')

const BeeDiffStream = require('../index')
const { streamToArray, setup, encodedOpen, jsonKeyedOpen, confirm, replicateAndSync } = require('./helpers')

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

  await confirm(bases)

  const newBee = base1.view.bee.snapshot()
  await newBee.ready()

  t.is(newBee.core.signedLength, 3) // Sanity check
  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee))

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-1', '1-2'])
  t.alike(diffs.map(({ right }) => right), [null, null])
})

test('new bee forked, but no old fork nor changes to index', async t => {
  const bases = await setup(t)
  const base1 = bases[0]

  const origBee = base1.view.bee.snapshot()
  await origBee.ready()

  await base1.append({ entry: ['1-1', '1-entry1'] })

  await base1.append({ entry: ['1-2', '1-entry2'] })

  await new Promise(resolve => setTimeout(resolve, 500))

  const newBee = base1.view.bee.snapshot()
  await newBee.ready()

  t.is(newBee.core.signedLength, 0) // Sanity check

  await new Promise(resolve => setTimeout(resolve, 500))
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
  await origBee.ready()

  const origIndexedL = origBee.core.signedLength
  t.is(origIndexedL, 0) // Sanity check

  await base1.append({ entry: ['1-3', '1-entry3'] })
  await base1.append({ entry: ['1-4', '1-entry4'] })

  const newBee = base1.view.bee.snapshot()
  await newBee.ready()

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee, { closeSnapshots: false }))
  t.is(newBee.core.signedLength, 0) // Sanity check

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-3', '1-4'])
  t.alike(diffs.map(({ right }) => right), [null, null])
})

test('both new index and new fork--old had up to date index', async t => {
  const bases = await setup(t)
  const [base1, base2, readOnlyBase] = bases

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await base1.append({ entry: ['1-2', '1-entry2'] })
  await confirm([base1, base2])
  await replicateAndSync(bases)

  const origBee = readOnlyBase.view.bee.snapshot()
  await origBee.ready()

  const origIndexedL = readOnlyBase.view.bee.core.signedLength
  t.is(origIndexedL, 3) // Sanity check
  t.is(origBee.version, 3) // Sanity check

  await base1.append({ entry: ['1-3', '1-entry3'] })
  await base1.append({ entry: ['1-4', '1-entry4'] })
  await confirm([base1, base2])

  // Fork
  await base1.append({ entry: ['1-5', '1-entry5'] })
  await replicateAndSync([base1, readOnlyBase])

  const newBee = readOnlyBase.view.bee.snapshot()
  await newBee.ready()

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee, { closeSnapshots: false }))
  t.is(newBee.feed.signedLength, 5) // Sanity check
  t.is(newBee.version, 6) // Sanity check

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-3', '1-4', '1-5'])
  t.alike(diffs.map(({ right }) => right), [null, null, null])
})

test('local version > 0, signedLength still 0--merge in remote fork', async t => {
  const bases = await setup(t)
  const [base1, base2, readOnlyBase] = bases

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await base1.append({ entry: ['1-2', '1-entry2'] })
  await base2.append({ entry: ['2-1', '2-entry1'] })

  const origBee = base2.view.bee.snapshot()
  await origBee.ready()

  const origIndexedL = base2.view.bee.core.signedLength
  t.is(origIndexedL, 0) // Sanity check
  t.is(origBee.version, 2) // Sanity check

  await replicateAndSync(bases)

  const newBee = readOnlyBase.view.bee.snapshot()
  await newBee.ready()

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee, { closeSnapshots: false }))
  t.is(newBee.feed.signedLength, 0) // Sanity check
  t.is(newBee.version, 4) // Sanity check

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-1', '1-2'])
  t.alike(diffs.map(({ right }) => right), [null, null])
})

test('new index, new fork and old fork all resolved nicely', async t => {
  const bases = await setup(t)
  const [base1, base2, readOnlyBase] = bases

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await confirm([base1, base2])
  await base1.append({ entry: ['1-2', '1-entry2'] })
  await replicateAndSync(bases)

  const origBee = readOnlyBase.view.bee.snapshot()
  await origBee.ready()

  const origIndexedL = readOnlyBase.view.bee.core.signedLength
  t.is(origIndexedL, 2) // Sanity check
  t.is(origBee.version, 3) // Sanity check

  await base1.append({ entry: ['1-3', '1-entry3'] })
  await base1.append({ entry: ['1-4', '1-entry4'] })
  await confirm([base1, base2])

  // New Fork
  await base1.append({ entry: ['1-5', '1-entry5'] })
  await replicateAndSync([base1, readOnlyBase])

  const newBee = readOnlyBase.view.bee.snapshot()
  await newBee.ready()

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee, { closeSnapshots: false }))
  t.is(newBee.feed.signedLength, 5) // Sanity check
  t.is(newBee.version, 6) // Sanity check

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-3', '1-4', '1-5'])
  t.alike(diffs.map(({ right }) => right), [null, null, null])
})

test('new index, new fork and old fork all resolved nicely (deletes)', async t => {
  const bases = await setup(t)
  const [base1, base2, readOnlyBase] = bases

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await confirm([base1, base2])
  await base1.append({ entry: ['1-2', '1-entry2'] })
  await replicateAndSync(bases)

  const origBee = readOnlyBase.view.bee.snapshot()
  await origBee.ready()

  const origIndexedL = readOnlyBase.view.bee.core.signedLength
  t.is(origIndexedL, 2) // Sanity check
  t.is(origBee.version, 3) // Sanity check

  await base1.append({ delete: '1-1' })
  await base1.append({ entry: ['1-3', '1-entry3'] })
  await base1.append({ entry: ['1-4', '1-entry4'] })
  await confirm([base1, base2])

  // New Fork
  await base1.append({ entry: ['1-5', '1-entry5'] })
  await base1.append({ delete: '1-3' })
  await replicateAndSync([base1, readOnlyBase])

  const newBee = readOnlyBase.view.bee.snapshot()
  await newBee.ready()

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee, { closeSnapshots: false }))
  t.is(newBee.feed.signedLength, 6) // Sanity check
  t.is(newBee.version, 8) // Sanity check

  t.alike(diffs.map(({ left }) => left?.key.toString()), [undefined, '1-4', '1-5'])
  t.alike(diffs.map(({ right }) => right?.key.toString()), ['1-1', undefined, undefined])
})

test('new snapshot has same final value as old fork but through different path ->no change', async t => {
  const bases = await setup(t, { openFun: encodedOpen })
  const [base1, base2] = bases

  await base1.append({ entry: ['shared', 'shared-entry'] })
  await base1.append({ entry: ['to-be-deleted', 'shared-delete'] })

  await confirm([base1, base2])

  // Both bases will modify shared to 'change' and will delete 'to-be-deleted'
  // but through a different series of operations

  await base1.append({ entry: ['shared', 'I'] })
  await base1.append({ entry: ['shared', 'like'] })
  await base1.append({ entry: ['shared', 'local'] })
  await base1.append({ entry: ['shared', 'change'] })
  await base1.append({ entry: ['to-be-deleted', 'about to be deleted'] })
  await base1.append({ delete: 'to-be-deleted' })

  const origBee = base1.view.bee.snapshot()
  await origBee.ready()

  // Normally base1 would now create the diffStream and yield the changes to this point
  // So reaching here, it has yielded 'change' and the deletion already

  // Now base2 also makes local changes to the same entries
  // ending up with the same net changes
  await base2.append({ entry: ['shared', 'Different path'] })
  await base2.append({ entry: ['shared', 'But same resulting'] })
  await base2.append({ entry: ['shared', 'change'] })
  await base2.append({ entry: ['something', 'else'] })
  await base2.append({ delete: 'to-be-deleted' })

  await confirm([base1, base2])

  const newBee = base1.view.bee.snapshot() // Need only yield 'something->else' as change
  await newBee.ready()

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee, { closeSnapshots: false }))
  const expected = [{ left: { seq: 10, key: 'something', value: 'else' }, right: null }]
  sameKeysAndValues(t, diffs, expected)
})

test('both old and new made changes to the same key -> new value yielded, but source = the old value', async t => {
  const bases = await setup(t, { openFun: encodedOpen })
  const [base1, base2] = bases

  await base1.append({ entry: ['shared', 'shared-entry'] })

  await confirm([base1, base2])
  // Both bases will modify 'shared''

  await base2.append({ entry: ['shared', 'I'] })
  await base2.append({ entry: ['shared', 'modify'] })

  const origBee = base2.view.bee.snapshot()
  await origBee.ready()

  // Normally base1 would now create the diffStream and yield the changes to this point
  // So reaching here, it has yielded 'modify' as current state

  // Now base2 also makes local changes to the same entry
  // ending up with a different value
  await base1.append({ entry: ['shared', 'Different path'] })
  await base1.append({ entry: ['shared', 'Different result'] })

  // The linearisation alg will make base2 win
  await confirm([base1, base2])

  const newBee = base1.view.bee.snapshot()
  await newBee.ready()

  t.is((await newBee.get('shared')).value, 'Different result') // Sanity check on linearisation order

  // the change to yield now is from base1's last value -> the current value
  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee, { closeSnapshots: false }))
  const sourceEntry = { seq: 3, key: 'shared', value: 'modify' }
  const destEntry = { seq: 5, key: 'shared', value: 'Different result' }
  sameKeysAndValues(t, diffs, [{ left: destEntry, right: sourceEntry }])
})

test('complex autobase linearisation with truncates', async t => {
  const bases = await setup(t)
  const [base1, base2] = bases

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await base1.append({ entry: ['1-2', '1-entry2'] })
  await confirm([base1, base2])

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
  await origBee.ready()

  const origIndexedL = origBee.core.signedLength
  t.is(origIndexedL, 3) // Sanity check
  t.is(origBee.version, 5) // Sanity check

  const origBee2 = base2.view.bee.snapshot()
  await origBee2.ready()

  const origIndexedL2 = origBee2.core.signedLength
  t.is(origIndexedL2, 3) // Sanity check
  t.is(origBee2.version, 6) // Sanity check

  await confirm([base1, base2])

  const newBee1 = base1.view.bee.snapshot()
  const newBee2 = base2.view.bee.snapshot()
  await newBee1.ready()
  await newBee2.ready()

  const diffsBee1 = await streamToArray(new BeeDiffStream(origBee, newBee1, { closeSnapshots: false }))
  const diffsBee2 = await streamToArray(new BeeDiffStream(origBee2, newBee2, { closeSnapshots: false }))

  t.is(newBee1.feed.signedLength, 8) // Sanity check
  t.is(newBee1.version, 8) // Sanity check
  t.alike(diffsBee1.map(({ left }) => left.key.toString()), ['2-1', '2-2', '2-3'])
  t.alike(diffsBee1.map(({ right }) => right), [null, null, null])

  t.is(newBee2.feed.signedLength, 8) // Sanity check
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
  await confirm([base1, base2])

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
  await origBee.ready()

  const origIndexedL = origBee.core.signedLength
  t.is(origIndexedL, 3) // Sanity check
  t.is(origBee.version, 7) // Sanity check

  const origBee2 = base2.view.bee.snapshot()
  await origBee2.ready()

  const origIndexedL2 = origBee2.core.signedLength
  t.is(origIndexedL2, 3) // Sanity check
  t.is(origBee2.version, 5) // Sanity check

  await confirm([base1, base2])

  const newBee1 = base1.view.bee.snapshot()
  const newBee2 = base2.view.bee.snapshot()

  await newBee1.ready()
  await newBee2.ready()

  const diffsBee1 = await streamToArray(new BeeDiffStream(origBee, newBee1, { closeSnapshots: false }))
  const diffsBee2 = await streamToArray(new BeeDiffStream(origBee2, newBee2, { closeSnapshots: false }))

  t.is(newBee1.feed.signedLength, 9) // Sanity check
  t.is(newBee1.version, 9) // Sanity check
  t.alike(diffsBee1.map(({ left }) => left.key.toString()), ['2-1', '2-2'])
  t.alike(diffsBee1.map(({ right }) => right), [null, null])

  t.is(newBee2.feed.signedLength, 9) // Sanity check
  t.is(newBee2.version, 9) // Sanity check
  t.alike(diffsBee2.map(({ left }) => left?.key.toString()), [undefined, '1-4'])
  t.alike(diffsBee2.map(({ right }) => right?.key.toString()), ['1-1', undefined]) // deletions

  // Sanity check: we did indeed truncate
  t.is(hasTruncated, true)
})

test('works with normal hyperbee', async function (t) {
  const bee = new Hyperbee(new Hypercore(await t.tmp()))
  await bee.put('e1', 'entry1')

  const oldSnap = bee.snapshot()
  await oldSnap.ready()

  await bee.put('e2', 'entry2')
  await bee.put('e3', 'entry3')
  await bee.del('e2')
  await bee.del('e1')

  const newSnap = bee.snapshot()
  await newSnap.ready()

  const directDiffs = await streamToArray(newSnap.createDiffStream(oldSnap.version))
  const diffs = await streamToArray(new BeeDiffStream(oldSnap, newSnap))

  t.alike(diffs.map(({ left }) => left?.key.toString()), [undefined, 'e3'])
  t.alike(diffs.map(({ right }) => right?.key.toString()), ['e1', undefined]) // deletions

  sameKeysAndValues(t, directDiffs, diffs)

  await bee.close()
})

test('can handle hyperbee without key or value encoding', async function (t) {
  const bases = await setup(t)

  const base1 = bases[0]
  const bee = base1.view.bee
  bee.keyEncoding = null
  bee.valueEncoding = null

  const oldBee = bee.snapshot()
  await oldBee.ready()

  await base1.append({ entry: ['1-1', '1-entry1'] })

  const newBee = bee.snapshot()
  await newBee.ready()

  const diffs = await streamToArray(new BeeDiffStream(oldBee, newBee))
  t.alike(diffs.map(({ left }) => left?.key), [b4a.from('1-1')])
  t.alike(diffs.map(({ right }) => right?.key), [undefined]) // deletions
})

test('yields with original encoding', async function (t) {
  const bases = await setup(t, { openFun: encodedOpen })

  const [base1, base2] = bases
  const bee = base1.view.bee

  await base1.append({ entry: ['1-1', { name: 'name1' }] })
  const oldBee = bee.snapshot()
  await oldBee.ready()

  await confirm([base1, base2])

  await base2.append({ entry: ['2-1', { name: '2-name1' }] })
  await base1.append({ entry: ['1-2', { name: 'name2' }] })
  await base1.append({ delete: '1-1' })

  await confirm([base1, base2])

  const newBee = bee.snapshot()
  await newBee.ready()

  const diff = await streamToArray(new BeeDiffStream(oldBee, newBee, { closeSnapshots: false }))
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
  sameKeysAndValues(t, diff, expected)
})

test('can pass diffStream range opts', async function (t) {
  const bases = await setup(t, { openFun: encodedOpen })

  const [base1, base2] = bases
  const bee = base1.view.bee

  await base1.append({ entry: ['1-1', { name: 'name1' }] })

  const oldBee = bee.snapshot()
  await oldBee.ready()

  await confirm([base1, base2])

  await base2.append({ entry: ['2-1', { name: '2-name1' }] })
  await base1.append({ entry: ['1-2', { name: 'name2' }] })
  await base1.append({ delete: '1-1' })

  await confirm([base1, base2])

  const newBee = bee.snapshot()
  await newBee.ready()

  const diff = await streamToArray(new BeeDiffStream(oldBee, newBee, {
    gt: '1-1',
    lt: '2-1',
    closeSnapshots: false
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
  sameKeysAndValues(t, diff, expected)
})

test('can pass in key- or valueEncoding', async function (t) {
  const bases = await setup(t)
  const base1 = bases[0]

  const bee = base1.view.bee
  const origBee = bee.snapshot()
  await origBee.ready()

  await base1.append({ entry: ['1-1', '1-entry1'] })

  t.alike((await bee.get('1-1')).key, b4a.from('1-1')) // Sanity check that encoding is binary

  const origBee1 = origBee.snapshot()
  const origBee2 = origBee.snapshot()
  const newBee1 = bee.snapshot()
  const newBee2 = bee.snapshot()

  await origBee1.ready()
  await origBee2.ready()
  await newBee1.ready()
  await newBee1.ready()

  const keyTextDiffs = await streamToArray(new BeeDiffStream(origBee1, newBee1, { keyEncoding: 'utf-8' }))
  const valueTextDiffs = await streamToArray(new BeeDiffStream(origBee2, newBee2, { valueEncoding: 'utf-8' }))

  sameKeysAndValues(t, keyTextDiffs, [{ left: { seq: 1, key: '1-1', value: b4a.from('1-entry1') }, right: null }])
  sameKeysAndValues(t, valueTextDiffs, [{ left: { seq: 1, key: b4a.from('1-1'), value: '1-entry1' }, right: null }])
})

test('reversing old- and new snapshot position yields reversed left-right', async function (t) {
  // We use a complex scenario to increase the probability of detecting potential issues
  const bases = await setup(t, { openFun: encodedOpen })
  const [base1, base2] = bases

  await base2.append({ entry: ['1-1', '1-entry1'] })
  await base2.append({ entry: ['1-2', '1-entry2'] })
  await base2.append({ entry: ['shared', 'shared-init'] })
  await base2.append({ entry: ['shared-del', 'to-be-deleted'] })
  await confirm([base1, base2])

  await Promise.all([
    base2.append({ entry: ['1-3', '1-entry3'] }),
    base2.append({ entry: ['1-4', '1-entry4'] }),
    base2.append({ entry: ['shared', 'temp-shared-value'] }),
    base2.append({ entry: ['shared', 'shared-final'] }),
    base2.append({ entry: ['shared-del', 'will be deleted in linearisation'] }),
    base1.append({ entry: ['2-1', '2-entry1'] }),
    base1.append({ entry: ['2-2', '2-entry2'] }),
    base1.append({ entry: ['2-3', '2-entry3'] }),
    base1.append({ entry: ['shared', 'to-be-insta-changed'] }),
    base1.append({ entry: ['shared', 'shared-final'] }),
    base1.append({ delete: 'shared-del' })
  ])

  const origBee = base2.view.bee.snapshot()
  const origBee2 = base1.view.bee.snapshot()

  await confirm([base1, base2])

  const newBee1 = base2.view.bee.snapshot()
  const newBee2 = base1.view.bee.snapshot()

  const origSnap = origBee.snapshot()
  const origSnap2 = origBee2.snapshot()

  const newSnap1 = newBee1.snapshot()
  const newSnap2 = newBee2.snapshot()

  await origSnap.ready()
  await origSnap2.ready()
  await newSnap1.ready()
  await newSnap2.ready()

  const diffsBee1 = await streamToArray(new BeeDiffStream(origSnap, newSnap1, { closeSnapshots: false }))
  const diffsBee2 = await streamToArray(new BeeDiffStream(origSnap2, newSnap2, { closeSnapshots: false }))

  const reverseDiffsBee1 = await streamToArray(new BeeDiffStream(newSnap1, origSnap))
  const reverseDiffsBee2 = await streamToArray(new BeeDiffStream(newSnap2, origSnap2))

  // Check they are indeed reversed (so left<->right)
  // DEVNOTE: if this fails because of different seqs,
  // consider not testing on the seqs
  // (but make sure it is indeed not guaranteed that they have the same seqs)
  t.is(diffsBee1.length, 4, 'sanity check')
  t.alike(diffsBee1, reverseDiffsBee1.map(({ left, right }) => { return { left: right, right: left } }))

  t.is(diffsBee2.length, 2) // sanity check
  t.alike(diffsBee2, reverseDiffsBee2.map(({ left, right }) => { return { left: right, right: left } }))
})

test('passed snapshots close when the beeDiffStream is destroyed', async t => {
  t.plan(2)
  const bases = await setup(t)
  const base1 = bases[0]

  const origBee = base1.view.bee.snapshot()
  await base1.append({ entry: ['1-1', '1-entry1'] })
  const newBee = base1.view.bee.snapshot()

  await origBee.ready()
  await newBee.ready()

  const diffStream = new BeeDiffStream(origBee, newBee)
  diffStream.on('close', () => {
    t.is(origBee.core.closed, true)
    t.is(newBee.core.closed, true)
  })
  diffStream.destroy()
})

test('correctly handles diff between snapshots older than the signedLength (autobase view)', async t => {
  const bases = await setup(t, { openFun: encodedOpen })

  const [base1, base2] = bases
  const bee = base1.view.bee

  await base1.append({ entry: ['1-1', { name: 'entry 1-1' }] })
  await base1.append({ entry: ['1-2', { name: 'entry 1-2' }] })
  await base1.append({ entry: ['1-3', { name: 'entry 1-3' }] })
  await base1.append({ entry: ['1-4', { name: 'entry 1-4' }] })

  await confirm([base1, base2])
  const oldBee = bee.checkout(2) // Post 1-1 added
  const newBee = bee.checkout(4) // Post 1-3 added

  await oldBee.ready()
  await newBee.ready()

  // Sanity check
  t.is(oldBee.core.signedLength, 5)
  t.is(oldBee.core.signedLength, newBee.core.signedLength)

  const diffs = await streamToArray(new BeeDiffStream(oldBee, newBee))
  t.alike(diffs.map(({ left }) => left?.key.toString()), ['1-2', '1-3'])
  t.alike(diffs.map(({ right }) => right?.key.toString()), [undefined, undefined]) // deletions
})

test('correctly handles diff between snapshots older than the signedLength (normal bee)', async function (t) {
  const bee = new Hyperbee(new Hypercore(await t.tmp()))
  await bee.put('e1', 'entry1')

  // v2
  await bee.put('e2', 'entry2')
  await bee.put('e3', 'entry3')
  await bee.del('e2')
  await bee.del('e1')
  // v6

  await bee.put('e4', 'entry4')
  await bee.put('e5', 'entry5')
  // v8

  const oldSnap = bee.checkout(2)
  const newSnap = bee.checkout(6)

  await oldSnap.ready()
  await newSnap.ready()

  const directDiffs = await streamToArray(newSnap.createDiffStream(oldSnap.version))
  const diffs = await streamToArray(new BeeDiffStream(oldSnap, newSnap))

  t.alike(diffs.map(({ left }) => left?.key.toString()), [undefined, 'e3'])
  t.alike(diffs.map(({ right }) => right?.key.toString()), ['e1', undefined]) // deletions

  sameKeysAndValues(t, directDiffs, diffs)

  await bee.close()
})

test('works with JSON key encoding', async t => {
  const bases = await setup(t, { openFun: jsonKeyedOpen })
  const [base1, base2, readOnlyBase] = bases

  const key = '0'.repeat(64)

  await base1.append({ entry: [{ key, seq: 0 }, '1-entry1'] })
  await confirm([base1, base2])
  await base1.append({ entry: [{ key, seq: 1 }, '1-entry2'] })
  await replicateAndSync(bases)

  const origBee = readOnlyBase.view.bee.snapshot()

  await base1.append({ entry: [{ key, seq: 2 }, '1-entry3'] })
  await base1.append({ entry: [{ key, seq: 3 }, '1-entry4'] })
  await confirm([base1, base2])

  // New Fork
  await base1.append({ entry: [{ key, seq: 4 }, '1-entry5'] })
  await replicateAndSync([base1, readOnlyBase])

  const newBee = readOnlyBase.view.bee.snapshot()

  await origBee.ready()
  await newBee.ready()

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee, { closeSnapshots: false }))

  t.alike(diffs.map(({ left }) => left.key), [{ key, seq: 2 }, { key, seq: 3 }, { key, seq: 4 }])
  t.alike(diffs.map(({ right }) => right), [null, null, null])
})

test('works with JSON key encoding and ranges', async t => {
  const bases = await setup(t, { openFun: jsonKeyedOpen })
  const [base1, base2, readOnlyBase] = bases

  const key = '0'.repeat(64)

  await base1.append({ entry: [{ key, seq: 0 }, '1-entry1'] })
  await confirm([base1, base2])
  await base1.append({ entry: [{ key, seq: 1 }, '1-entry2'] })
  await replicateAndSync(bases)

  const origBee = readOnlyBase.view.bee.snapshot()

  await base1.append({ entry: [{ key, seq: 2 }, '1-entry3'] })
  await base1.append({ entry: [{ key, seq: 3 }, '1-entry4'] })
  await confirm([base1, base2])

  // New Fork
  await base1.append({ entry: [{ key, seq: 4 }, '1-entry5'] })
  await replicateAndSync([base1, readOnlyBase])

  const newBee = readOnlyBase.view.bee.snapshot()

  await origBee.ready()
  await newBee.ready()

  const diffs = await streamToArray(new BeeDiffStream(origBee, newBee, { gte: { key, seq: 3, closeSnapshots: false } }))

  t.alike(diffs.map(({ left }) => left.key), [{ key, seq: 3 }, { key, seq: 4 }])
  t.alike(diffs.map(({ right }) => right), [null, null])
})

test('does not close snapshots if option set', async function (t) {
  const bee = new Hyperbee(new Hypercore(await t.tmp()))

  await bee.put('e1', 'entry1')
  const oldSnap = bee.snapshot()
  const oldSnapRef = bee.snapshot()

  await bee.put('e2', 'entry2')
  const newSnap = bee.snapshot()
  const newSnapRef = bee.snapshot()

  await oldSnap.ready()
  await oldSnapRef.ready()

  await newSnap.ready()
  await newSnapRef.ready()

  await streamToArray(new BeeDiffStream(oldSnap, newSnap, { closeSnapshots: false }))
  t.is(oldSnap.core.closed, false)
  t.is(newSnap.core.closed, false)

  // default: true
  await streamToArray(new BeeDiffStream(oldSnapRef, newSnapRef))
  t.is(oldSnapRef.core.closed, true)
  t.is(newSnapRef.core.closed, true)

  await oldSnap.close()
  await newSnap.close()
  await bee.close()
})

test('supports diffing values skipped by hyperbee encoding', async t => {
  const [base1] = await setup(t, { openFun: encodedOpen })
  const bee = base1.view.bee

  const oldBee = bee.snapshot()
  // Append key w/ no value
  await base1.append({ entry: ['1-1'] })
  await base1.append({ entry: ['1-2', undefined] })

  const newBee = bee.snapshot()

  await oldBee.ready()
  await newBee.ready()

  const diffs = await streamToArray(new BeeDiffStream(oldBee, newBee))
  t.alike(diffs.map(({ left }) => left), [
    {
      seq: 1,
      key: '1-1',
      value: null
    },
    {
      seq: 2,
      key: '1-2',
      value: null
    }
  ])
  t.alike(diffs.map(({ right }) => right?.key), [undefined, undefined]) // deletions
})

test('throws if a snapshot is not opened', async t => {
  const store = new Corestore(await tmpDir())
  const core = store.get({ name: 'bee' })
  const bee = new Hyperbee(core)
  t.exception(
    () => new BeeDiffStream(bee, bee),
    /Snapshot must be opened/,
    'none open'
  )

  await bee.ready()
  const snap = bee.snapshot()

  t.exception(
    () => new BeeDiffStream(bee, snap),
    /Snapshot must be opened/,
    'right not open'
  )
  t.exception(
    () => new BeeDiffStream(snap, bee),
    /Snapshot must be opened/,
    'left not open'
  )

  await snap.ready()
  t.execution(() => new BeeDiffStream(snap, bee), 'sanity check that happy path does not throw')
})

function sameKeysAndValues (t, actual, expected) {
  const extractKeysAndValues = ({ left, right }) => {
    const res = {}
    if (left !== null) {
      if (!left.key || !left.value) throw new Error('Invalid entry')
      res.left = { key: left.key, value: left.value }
    }
    if (right !== null) {
      if (!right.key || !right.value) throw new Error('Invalid entry')
      res.right = { key: right.key, value: right.value }
    }
    if (!res.left && !res.right) throw new Error('Invalid entry')

    return res
  }

  const actualClean = actual.map(extractKeysAndValues)
  const expectedClean = expected.map(extractKeysAndValues)

  t.alike(actualClean, expectedClean)
}
