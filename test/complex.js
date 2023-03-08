const test = require('brittle')
const Hyperbee = require('hyperbee')
const ram = require('random-access-memory')
const Hypercore = require('hypercore')
const { performance } = require('perf_hooks')

const BeeDiffStream = require('../index')
const { streamToArray, setup, encodedOpen, confirm } = require('./helpers')

test('complex scenario with many diff cases', async t => {
  const bases = await setup(t, { openFun: encodedOpen })
  const [base1, base2] = bases

  // Baseline
  const baselineProms = []
  for (let i = 1; i <= 6; i++) {
    baselineProms.push(base1.append({ entry: [`1-${i}`, `1-${i} init`] }))
    baselineProms.push(base1.append({ entry: [`shared-${i}`, `shared-${i} init`] }))
  }
  await Promise.all(baselineProms)

  await confirm(base1, base2)

  // Corresponds to the state just before going offline
  const baselineBee = base2.view.bee.snapshot()

  t.is(base2.view.bee.core.indexedLength, 13) // Sanity check

  // Continue working offline, thereby creating a local fork
  await Promise.all([
    base2.append({ entry: ['shared-1', 'shared-1 modified once by 2'] }),
    base2.append({ entry: ['shared-2', 'shared-2 modified multiple times by 2'] }),
    base2.append({ entry: ['shared-3', 'shared-3 modified then deleted by 2'] }),
    // share-4 left unchanged
    base2.append({ delete: 'shared-5' }), // deleted
    base2.append({ delete: 'shared-6' }), // deleted then restored
    base2.append({ entry: ['shared-new', 'shared-new added by 2'] }),
    base2.append({ entry: ['shared-new2', 'shared-new2 added by 2'] }),

    base2.append({ entry: ['2-1', '2-1 added by 2'] }),
    base2.append({ entry: ['2-2', '2-2 added and mofified 2'] }),
    base2.append({ entry: ['2-3', '2-3 added then deleted by 2'] })
  ])

  await base2.append({ entry: ['shared-2', 'shared-2 modified multiple times by 2 (2)'] })
  await base2.append({ entry: ['shared-2', 'shared-2 modified multiple times by 2 (3)'] })
  await base2.append({ delete: 'shared-3' })
  await base2.append({ entry: ['shared-6', 'shared-6 deleted then restored by 2'] })

  await base2.append({ entry: ['2-2', '2-2 added and mofified (2)'] })
  await base2.append({ delete: '2-3' })

  // State just before reconnecting with base1
  const refBee = base2.view.bee.snapshot()

  const [refNewState, refOldState] = diffsToValues(
    await streamToArray(new BeeDiffStream(baselineBee, refBee.snapshot()))
  )
  t.alike(refNewState, [
    '2-1 added by 2',
    '2-2 added and mofified (2)',
    // no 2-3 because deleted again
    'shared-1 modified once by 2',
    'shared-2 modified multiple times by 2 (3)',
    null, // shared 3 deleted
    // shared-4 unchanged
    null, // shared-5 deleted
    'shared-6 deleted then restored by 2',
    'shared-new added by 2',
    'shared-new2 added by 2'
  ])
  t.alike(refOldState, [
    null, // 2-1 added
    null, // 2-2 added
    // no 2-3 because deleted again
    'shared-1 init',
    'shared-2 init',
    'shared-3 init',
    // shared-4 unchanged
    'shared-5 init',
    'shared-6 init',
    null, // 'shared-new added',
    null // shared-new2 added'
  ])

  // Now we simulate the actions of the unconnected other peer
  await Promise.all([
    base2.append({ entry: ['shared-1', 'shared-1 overruled by 1'] }),
    // shared-2 left unchanged
    base2.append({ delete: 'shared-3' }), // deleted by both 1 and 2
    // share-4 left unchanged by both
    base2.append({ entry: ['shared-5', 'shared-5 deletion overruled by 1'] }),
    base2.append({ entry: ['shared-6', 'shared-6 overruled with multiple entries by 1'] }),

    base2.append({ entry: ['shared-new', 'shared-new overruled by 1'] }),
    // shared-new2 left unchanged
    base2.append({ entry: ['shared-new3', 'shared-new3 added multiple times by 2'] }),

    base2.append({ entry: ['1-1', '1-1 updated by 1'] }),
    base2.append({ delete: '1-2' })
  ])

  await base2.append({ entry: ['shared-6', 'shared-6 overruled with multiple entries by 1 (2)'] })
  await base2.append({ entry: ['shared-new3', 'shared-new3 added multiple times by 2 (2)'] })

  // The peers sync and the autobases are linearised
  await confirm(base1, base2)
  const newBee = base2.view.bee.snapshot()
  const [newState, refState] = diffsToValues(
    await streamToArray(new BeeDiffStream(refBee, newBee))
  )

  t.alike(newState, [
    '1-1 updated by 1',
    null, // 1-2 deleted
    // No changes to 2's non-shared entries
    'shared-1 overruled by 1',
    // shared-2: no change by 1, and 2's change already handled
    // shared-3: deleted by both 1 and 2, so no change
    // shared-4: no change by anyone
    'shared-5 deletion overruled by 1',
    'shared-6 overruled with multiple entries by 1 (2)',
    'shared-new overruled by 1',
    // No change to shared-new2
    'shared-new3 added multiple times by 2 (2)'
  ])
  t.alike(refState, [
    '1-1 init',
    '1-2 init',
    'shared-1 modified once by 2',
    // no changes for shared-2 -> shared-4
    null, // overruled deletion of shared-5
    'shared-6 deleted then restored by 2',
    'shared-new added by 2', // overruled
    null // shared-new3
  ])
})

function diffsToValues (diffs) {
  const newState = diffs.map(d => d.left ? d.left.value : d.left)
  const oldState = diffs.map(d => d.right ? d.right.value : d.right)
  return [newState, oldState]
}

test('low overhead compared to normal diffStream if applied to bee', async t => {
  const maxOverheadFactor = 2
  const magnitudes = [50] // [1, 10, 100, 1000]

  const propInitEntries = 10
  const propTotalEntries = 100
  const repeatFactor = 1

  for (const magnitude of magnitudes) {
    const nrInitEntries = propInitEntries * magnitude
    const nrTotalEntries = propTotalEntries * magnitude

    const db = new Hyperbee(new Hypercore(ram))

    // Setup bee
    const initBatch = db.batch()
    const initProms = []
    for (let i = 0; i < nrInitEntries; i++) initProms.push(initBatch.put(`e${i}`, `e${i}-init`))
    await Promise.all(initProms)
    await initBatch.flush()

    const oldSnap = db.snapshot()
    t.is(oldSnap.version, nrInitEntries + 1)

    for (let rep = 0; rep < repeatFactor; rep++) {
      const batch = db.batch()
      const proms = []
      for (let i = 0; i < nrTotalEntries; i++) proms.push(batch.put(`e${i}`, `e${i}-rep${rep}`))
      await Promise.all(proms)
      await batch.flush()
    }

    const newSnap = db.snapshot()
    t.is(newSnap.version, oldSnap.version + repeatFactor * nrTotalEntries) // Sanity check

    // Do diff stream measurements
    const startOwn = performance.now()
    const differDiffs = await streamToArray(new BeeDiffStream(oldSnap.snapshot(), newSnap.snapshot()))
    const ownTime = performance.now() - startOwn

    const startBee = performance.now()
    const beeDiffs = await streamToArray(newSnap.createDiffStream(oldSnap.version))
    const beeTime = performance.now() - startBee

    t.is(beeDiffs.length, nrTotalEntries) // Sanity check
    t.alike(differDiffs, beeDiffs) // Sanity check

    t.is(ownTime < beeTime * maxOverheadFactor, true)
  }
})
