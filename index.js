const Union = require('sorted-union-stream')
const b4a = require('b4a')

function getKey (diffEntry) {
  const { left, right } = diffEntry
  return left ? left.key : right.key
}

function areEqual (diff1, diff2) {
  if (diff1 === null && diff2 === null) return true
  if (diff1 === null || diff2 === null) return false
  return b4a.equals(diff1.value, diff2.value)
}

function getDiffs (oldBee, newBee) {
  // For easier comparisons of the values
  oldBee = oldBee.snapshot({ keyEncoding: 'binary', valueEncoding: 'binary' })
  newBee = newBee.snapshot({ keyEncoding: 'binary', valueEncoding: 'binary' })

  const oldIndexedL = oldBee.core.indexedLength
  const oldDiffStream = oldBee.createDiffStream(oldIndexedL)
  const newDiffStream = newBee.createDiffStream(oldIndexedL)

  const unionised = new Union(
    oldDiffStream,
    newDiffStream,
    {
      compare: (e1, e2) => b4a.compare(getKey(e1), getKey(e2)),
      map: (oldEntry, newEntry) => {
        if (oldEntry === null) return newEntry
        // Old entries require undoing, so reverse
        if (newEntry === null) return { seq: oldEntry.seq, left: oldEntry.right, right: oldEntry.left }

        const leftEq = areEqual(oldEntry.left, newEntry.left)
        const rightEq = areEqual(oldEntry.right, newEntry.right)
        if (!(leftEq && rightEq)) return rightEq
        // else: already processed in prev getDiffs, so filter out
        return null
      }
    }
  )

  return unionised
}

module.exports = getDiffs
