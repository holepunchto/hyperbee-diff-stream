const sameObject = require('same-object')
const Union = require('sorted-union-stream')
const b4a = require('b4a')
const { Readable, Transform, pipeline } = require('streamx')

function getKey (diffEntry) {
  const { left, right } = diffEntry
  return left ? left.key : right.key
}

async function getDiffs (oldBee, newBee) {
  const oldIndexedL = oldBee.core.indexedLength

  const origOldDiffStream = oldBee.createDiffStream(oldIndexedL)
  const oldDiffMapper = new Transform({
    // left <-> right (~add<->delete) because the oldstream's entries need to be undone
    transform: (entry, cb) => cb(null, { left: entry.right, right: entry.left, isOld: true })
  })
  const oldDiffStream = pipeline(origOldDiffStream, oldDiffMapper)

  const newDiffStream = newBee.createDiffStream(oldIndexedL)
  const unionised = new Union(oldDiffStream, newDiffStream, (entry1, entry2) => {
    const res = b4a.compare(getKey(entry1), getKey(entry2))
    if (res !== 0) return res
    if (entry1.isOld === entry2.isOld) return 0
    return entry1.isOld ? -1 : 1 // Old first
  })

  const outStream = new Readable()

  let bufferedEntry = null
  const processEntry = (entry) => {
    if (bufferedEntry && b4a.equals(getKey(bufferedEntry), getKey(entry))) {
      const oldEntry = bufferedEntry
      const newEntry = entry

      // TODO: binary checkouts for b4a equals
      const leftEq = sameObject(oldEntry.right?.value, newEntry.left?.value)
      const rightEq = sameObject(oldEntry.left?.value, newEntry.right?.value)
      if (!(leftEq && rightEq)) { // else: already processed in prev getDiffs
        outStream.push(entry)
      }
      bufferedEntry = null
    } else {
      if (bufferedEntry) outStream.push(bufferedEntry)
      bufferedEntry = entry
    }
  }

  unionised.on('data', processEntry)
  unionised.on('close', () => {
    if (bufferedEntry) outStream.push(bufferedEntry)
    outStream.push(null)
  })

  return outStream
}

module.exports = getDiffs
