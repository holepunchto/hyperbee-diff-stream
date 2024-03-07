const ram = require('random-access-memory')
const Corestore = require('corestore')
const Hyperbee = require('hyperbee')
const helpers = require('autobase-test-helpers')
const Autobase = require('autobase')

const sync = helpers.sync
const DEBUG_LOG = false

module.exports = {
  create,
  sync,
  open,
  encodedOpen,
  jsonKeyedOpen,
  setup,
  streamToArray,
  confirm,
  replicateAndSync: helpers.replicateAndSync
}

async function createBase (store, key, apply, open, t, opts = {}) {
  const moreOpts = {
    apply,
    open,
    close: undefined,
    valueEncoding: 'json',
    ...opts
  }

  const base = new Autobase(store.session(), key, moreOpts)
  await base.ready()

  t.teardown(() => base.close(), { order: 1 })

  return base
}

async function createStores (n, t) {
  const stores = []
  for (let i = 0; i < n; i++) {
    const storage = ram.reusable()
    const primaryKey = Buffer.alloc(32, i)
    stores.push(new Corestore(storage, { primaryKey }))
  }

  t.teardown(() => Promise.all(stores.map(s => s.close())), { order: 2 })

  return stores
}

async function create (n, apply, open, t) {
  const stores = await createStores(n, t)
  const bases = [await createBase(stores[0], null, apply, open, t)]

  if (n === 1) return { stores, bases }

  for (let i = 1; i < n; i++) {
    bases.push(await createBase(stores[i], bases[0].local.key, apply, open, t))
  }

  return bases
}

async function addWriter (base, add, indexer = true) {
  return base.append({ add: add.local.key.toString('hex'), indexer })
}

async function addWriterAndSync (base, add, indexer = true, bases = [base, add]) {
  await addWriter(base, add, indexer)
  await helpers.replicateAndSync(bases)
  await base.ack()
  await helpers.replicateAndSync(bases)
}

async function setup (t, { openFun = open } = {}) {
  // 2 writers, 1 read-only
  const bases = await create(3, (...args) => apply(t, ...args), openFun, t)
  const [base1, base2] = bases

  await addWriterAndSync(base1, base2, true)
  await confirm(bases)

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
    if (DEBUG_LOG) console.debug('applying', value)
    if (value === null) continue
    if (value.add) {
      await base.addWriter(Buffer.from(value.add, 'hex'), { indexer: value.indexer })
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

async function confirm (bases, options = {}) {
  await helpers.replicateAndSync(bases)

  for (let i = 0; i < 2; i++) {
    const writers = bases.filter(b => !!b.localWriter)
    const maj = options.majority || (Math.floor(writers.length / 2) + 1)
    for (let j = 0; j < maj; j++) {
      if (!writers[j].writable) continue

      await writers[j].append(null)
      await helpers.replicateAndSync(bases)
    }
  }

  await helpers.replicateAndSync(bases)
}

function jsonKeyedOpen (linStore, base) {
  return new SimpleView(base, linStore.get('simple-bee'), {
    keyEncoding: 'json',
    valueEncoding: 'json'
  })
}
