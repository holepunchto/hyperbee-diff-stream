const BeeDiffStream = require('.')
const Hyperbee = require('hyperbee')
const ram = require('random-access-memory')
const Autobase = require('autobase-next')
const Corestore = require('corestore')

async function main () {
  const opts = {
    apply: async (batch, view) => {
      try {
        for (const { value } of batch) {
          if (value.add) await view.put(...value.add, { update: false })
          else if (value.del) await view.del(value.del, { update: false })
        }
      } catch (e) {
        console.error(e)
      }
    },
    open: linStore => new Hyperbee(linStore.get('abee'), { extension: false }),
    valueEncoding: 'json'
  }

  const store = new Corestore(ram)
  const base = new Autobase(store, null, opts)
  await base.append({ add: ['e1', 'entry1'] })
  await base.append({ add: ['e2', 'entry2'] })
  const oldSnap = base.view.snapshot()

  await base.append({ add: ['e3', 'entry3'] })
  await base.append({ del: 'e1' })
  await base.append({ del: 'e2' })
  await base.append({ add: ['e1', 'new entry1'] })

  const newSnap = base.view.snapshot()
  console.log('old version:', oldSnap.version, '--new version:', newSnap.version)

  const diffStream = new BeeDiffStream(oldSnap, newSnap)
  for await (const { left: added, right: removed } of diffStream) {
    if (added && !removed) console.log('Added:', added.key.toString())
    if (!added && removed) console.log('Removed: ', removed.key.toString())
    if (added && removed) console.log('Updated', added.key.toString(), 'from ', removed.value.toString(), ' to', added.value.toString())
  }
}

main()
