const test = require('brittle')

const { create, sync } = require('./helpers')

function printIndexLengths (msg, bases) {
  console.log(msg, bases.map(b => b.view.indexedLength))
}

test('basic - two writers', async t => {
  const bases = await create(3, apply, store => store.get('test'))
  const [base1, base2, base3] = bases

  printIndexLengths('Init state', bases)

  // Add base2 as writer
  await base1.append({
    add: base2.local.key.toString('hex')
  })
  await sync(base1, base2, base3)
  await base1.append(null)
  await sync(base1, base2, base3)
  printIndexLengths('Synced the add-writer msg:', bases)

  // Each writer adds a message
  await base1.append('1-entry1')
  await base2.append('2-entry1')
  printIndexLengths('Added new messages, but not synced:', bases)

  // Sync writers, but not reader
  await sync(base1, base2, base3)
  // Communicate desire to commit current state
  await base1.append(null)
  await base2.append(null)
  await sync(base1, base2)
  // Confirm desire to commit current state
  await base1.append(null)
  await base2.append(null)
  await sync(base1, base2)
  printIndexLengths('Synced 2 writers, but not reader:', bases)

  // Sync reader with one (or both) of the writers
  await sync(base1, base3)
  printIndexLengths('Reader synced:', bases)
})

async function apply (batch, view, base) {
  for (const { value } of batch) {
    if (value === null) continue
    // console.log(value)
    if (value.add) {
      await base.system.addWriter(Buffer.from(value.add, 'hex'))
    }
    if (view) await view.append(value)
  }
}
