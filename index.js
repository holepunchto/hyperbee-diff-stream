const toArray = require('stream-to-array')

function getChangedKey (diffEntry) {
  const { left, right } = diffEntry
  return left ? left.key.toString() : right.key.toString()
}

function getChangedSeqNr (diffEntry) {
  const { left, right } = diffEntry
  return left ? left.seq : right.seq
}

async function getDiffs (oldBee, newBee, oldIndexedL) {
  // Note: we need to explicitly pass the old indexed length,
  // because even on a snapshot bee the underlying core gets updated

  const newApplyDiff = await toArray(
    newBee.createDiffStream(oldIndexedL)
  )
  const oldToUndoDiff = await toArray(
    oldBee.createDiffStream(oldIndexedL)
  )

  const newKeys = new Set(newApplyDiff.map(getChangedKey))
  const oldSeqs = new Set(oldToUndoDiff.map(getChangedSeqNr))

  const res = []
  for (const newEntry of newApplyDiff) {
    const seq = newEntry.left?.seq || newEntry.right?.seq
    if (!oldSeqs.has(seq)) {
      res.push(newEntry)
    }
  }

  for (const entry of oldToUndoDiff) {
    const key = getChangedKey(entry)
    if (!newKeys.has(key)) {
      res.push({ seq: entry.seq, left: entry.right, right: entry.left })
    }
  }

  // No fork for old bee (note: init indexedL is 0, but init version 1)
  if (oldBee.version === oldIndexedL || oldIndexedL === 0) {
    return res
  }

  throw new Error('not implemented')
}

module.exports = getDiffs
