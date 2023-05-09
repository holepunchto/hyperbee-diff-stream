const ram = require('random-access-memory')
const Corestore = require('corestore')
const Hyperbee = require('hyperbee')

const Autobase = require('@holepunchto/autobase-next')

module.exports = {
  create,
  sync,
  open,
  encodedOpen,
  jsonKeyedOpen,
  setup,
  streamToArray,
  confirm
}

async function create (n, apply, open) {
  const opts = { apply, open, valueEncoding: 'json' }
  const bases = [new Autobase(new Corestore(ram, { primaryKey: Buffer.alloc(32).fill(0) }), null, opts)]
  await bases[0].ready()
  if (n === 1) return bases
  for (let i = 1; i < n; i++) {
    const base = new Autobase(new Corestore(ram, { primaryKey: Buffer.alloc(32).fill(i) }), bases[0].local.key, opts)
    await base.ready()
    bases.push(base)
  }
  return bases
}

async function sync (...bases) {
  const streams = []

  for (const a of bases) {
    for (const b of bases) {
      if (a === b) continue
      const s1 = a.store.replicate(true)
      const s2 = b.store.replicate(false)

      s1.on('error', () => {})
      s2.on('error', () => {})

      s1.pipe(s2).pipe(s1)

      streams.push(s1)
      streams.push(s2)
    }
  }

  await Promise.all(bases.map(b => b.update({ wait: true })))

  for (const stream of streams) {
    stream.destroy()
  }
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

async function confirm (base1, base2) {
  await sync(base1, base2)
  await base1.append(null)
  await base2.append(null)
  await sync(base1, base2)
  await base1.append(null)
  await base2.append(null)
  await sync(base1, base2)
}

function jsonKeyedOpen (linStore, base) {
  return new SimpleView(base, linStore.get('simple-bee'), {
    keyEncoding: 'json',
    valueEncoding: 'json'
  })
}
