const test = require('brittle')
const Hyperbee = require('hyperbee')

const getDiffs = require('../index')
const { create, sync } = require('./helpers')

test('no changes -> empty diff', async t => {
  const bases = await setup(t)
  const [base1] = bases

  const diffs = await getDiffs(base1.view.bee, base1.view.bee, base1.view.bee.core.indexedLength)
  t.is(diffs.length, 0)
})

test('index moved ahead', async t => {
  const bases = await setup(t)
  const base1 = bases[0]

  const origBee = base1.view.bee.snapshot()
  const origIndexedL = base1.view.bee.core.indexedLength

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await base1.append({ entry: ['1-2', '1-entry2'] })

  await confirm(...bases)

  const newBee = base1.view.bee.snapshot()

  t.is(newBee.core.indexedLength, 3) // Sanity check
  const diffs = await getDiffs(origBee, newBee, origIndexedL)

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-1', '1-2'])
  t.alike(diffs.map(({ right }) => right), [null, null])
})

test('new forked, but no old fork nor changes to index', async t => {
  const bases = await setup(t)
  const base1 = bases[0]

  const origBee = base1.view.bee.snapshot()
  const origIndexedL = base1.view.bee.core.indexedLength

  await base1.append({ entry: ['1-1', '1-entry1'] })
  await base1.append({ entry: ['1-2', '1-entry2'] })

  const newBee = base1.view.bee.snapshot()

  t.is(newBee.core.indexedLength, 0) // Sanity check
  const diffs = await getDiffs(origBee, newBee, origIndexedL)

  t.alike(diffs.map(({ left }) => left.key.toString()), ['1-1', '1-2'])
  t.alike(diffs.map(({ right }) => right), [null, null])
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
  const bases = await create(2, apply, open)
  const [base1, base2] = bases

  await base1.append({
    add: base2.local.key.toString('hex')
  })

  await sync(...bases)
  await base1.append(null)
  await sync(...bases)

  return [base1, base2]
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
    await this.bee.put(key, value, { update: false }) //, keyEncoding: 'binary', valueEncoding: 'binary' })
  }
}

function open (linStore, base) {
  // console.log('open params:')
  // console.log('own store ', ownStore)
  // console.log('lin store:', linStore)
  // console.log('base:', base)
  const core = linStore.get('simple-bee', { valueEncoding: 'binary' })

  const view = new SimpleView(base, core)
  return view
}

async function apply (batch, simpleView, base) {
  // console.log('view:', simpleView)
  // console.log('batch:', batch.map(b => b.value))

  for (const { value } of batch) {
    if (value === null) continue
    if (value.add) {
      await base.system.addWriter(Buffer.from(value.add, 'hex'))
    } else {
      // console.log('value for simple view:', value)
      // console.log('bee:', simpleView.bee)
      try {
        // console.log('val:', value)

        // console.log('Applying message')
        await simpleView._applyMessage(...value.entry)
      } catch (e) {
        console.error('ERROR\n\n\n', e)
      }
    }
  }
}
