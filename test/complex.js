const test = require('brittle')
const Hyperbee = require('hyperbee')
const ram = require('random-access-memory')
const Hypercore = require('hypercore')
const { performance } = require('perf_hooks')

const BeeDiffStream = require('../index')
const { streamToArray } = require('./helpers')

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
