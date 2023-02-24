const ram = require('random-access-memory')
const Corestore = require('corestore')

const Autobase = require('autobase-next')

module.exports = {
  create,
  sync
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
  await new Promise(resolve => setTimeout(resolve, 100))

  for (const stream of streams) {
    stream.destroy()
  }
}
