const BeeDiffStream = require('.')
const Hyperbee = require('hyperbee')
const ram = require('random-access-memory')
const Autobase = require('autobase-next')
const Corestore = require('corestore')

async function main () {
  // Setting up a simple hyperbee view for an autobase
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
    open: linStore => new Hyperbee(linStore.get('abee'), {
      extension: false,
      keyEncoding: 'utf-8',
      valueEncoding: 'utf-8'
    }),
    valueEncoding: 'json' // the apply function will receive batches of jsons
  }

  const store = new Corestore(ram)
  const base = new Autobase(store, null, opts)
  await base.append({ add: ['e1', 'entry1'] })
  await base.append({ add: ['e2', 'entry2'] })

  const oldSnap = base.view.snapshot()

  await base.append({ add: ['e3', 'A'] })
  await base.append({ add: ['e3', 'lot'] })
  await base.append({ add: ['e3', 'of'] })
  await base.append({ add: ['e3', 'changes'] })
  await base.append({ add: ['e3', 'to'] })
  await base.append({ add: ['e3', 'entry3'] })

  await base.append({ del: 'e1' })

  await base.append({ add: ['e2', 'new entry 2'] })
  await base.append({ del: 'e2' })

  await base.append({ add: ['e1', 'Something-else'] })

  const newSnap = base.view.snapshot()

  const diffStream = new BeeDiffStream(oldSnap, newSnap)
  for await (const { left: added, right: removed } of diffStream) {
    if (added && !removed) console.log('- Set', added.key, 'to', added.value)
    if (!added && removed) console.log('- Removed', removed.key)
    if (added && removed) console.log('- Updated', added.key, 'from', removed.value, 'to', added.value)
  }

  /*
    - Updated e1 from entry1 to Something-else
    - Removed e2
    - Added e3 entry3
  */
}

main()
