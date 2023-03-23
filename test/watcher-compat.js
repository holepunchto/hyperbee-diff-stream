const test = require('brittle')
const { streamToArray, setup, confirm } = require('./helpers')

test('compat - watch works with autobase bees', async function (t) {
  // bee.watch() works very well with this module, so we add a test
  // that ensures it remains compatible.
  // The main possible compat issue is autobase-next
  const bases = await setup(t)
  const [base1, base2] = bases

  // Make base2 writer too
  await base1.append({ add: base2.local.key.toString('hex') })
  await confirm(base1, base2)

  const bee = base1.view.bee // bee based on autobase linearised core

  const partialWatcher = bee.watch()
  const fullWatcher = bee.watch()
  const initBee = bee.snapshot()

  // Start consuming the watchers
  const consumePartialWatcherProm = consumeWatcher(partialWatcher)
  const consumeFullWatcherProm = consumeWatcher(fullWatcher)

  // Add shared entry
  await base1.append({ entry: ['1-1', '1-entry1'] })
  await confirm(base1, base2)

  await partialWatcher.destroy()
  const partialDiffs = await consumePartialWatcherProm

  // Init state
  t.alike(initBee.version, partialDiffs[0].previous.version)
  // Final state
  const partialFinal = await streamToArray(partialDiffs[partialDiffs.length - 1].current.createReadStream())
  t.alike(partialFinal.length, 1) // Sanity check
  t.alike(partialFinal, await streamToArray(bee.createReadStream()))

  await Promise.all([
    base1.append({ entry: ['1-2', '1-entry2'] }),
    base2.append({ entry: ['2-1', '2-entry1'] }),
    base2.append({ entry: ['2-2', '2-entry2'] })
  ])
  await confirm(base1, base2)

  await fullWatcher.destroy()
  const fullDiffs = await consumeFullWatcherProm

  // sanity check. Even though the exact amount is non-deterministic
  // it should have been triggered at least a few times.
  t.is(fullDiffs.length > 1, true)
  t.alike(
    await streamToArray(fullDiffs[0].previous.createReadStream()),
    await streamToArray(initBee.createReadStream())
  )
  // Final state
  const finalEntries = await streamToArray(fullDiffs[fullDiffs.length - 1].current.createReadStream())
  t.is(finalEntries.length, 4) // Sanity check
  t.alike(finalEntries, await streamToArray(bee.createReadStream()))
})

async function consumeWatcher (watcher) {
  const entries = []
  for await (const { current, previous } of watcher) {
    entries.push({ previous, current })
  }
  return entries
}
