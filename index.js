const sameObject = require('same-object')

function getChangedKey (diffEntry) {
  const { left, right } = diffEntry
  return left ? left.key.toString() : right.key.toString()
}

function shouldAddNewEntry (newEntry, oldEntries) {
  const newKey = getChangedKey(newEntry)
  if (!oldEntries.has(newKey)) return true

  const oldEntry = oldEntries.get(newKey)

  const leftEq = sameObject(oldEntry.left?.value, newEntry.left?.value)
  const rightEq = sameObject(oldEntry.right?.value, newEntry.right?.value)
  return !(leftEq && rightEq) // Was already processed
}

async function getDiffs (oldBee, newBee, oldIndexedL) {
  // Note: we need to explicitly pass the old indexed length,
  // because even on a snapshot bee the underlying core gets updated

  const newApplyDiff = new Map()
  for await (const entry of newBee.createDiffStream(oldIndexedL)) {
    newApplyDiff.set(getChangedKey(entry), entry)
  }

  const oldToUndoDiff = new Map()
  for await (const entry of oldBee.createDiffStream(oldIndexedL)) {
    oldToUndoDiff.set(getChangedKey(entry), entry)
  }

  const res = []
  for (const newEntry of newApplyDiff.values()) {
    if (shouldAddNewEntry(newEntry, oldToUndoDiff)) {
      res.push(newEntry)
    }
  }

  for (const [key, entry] of oldToUndoDiff) {
    if (!newApplyDiff.has(key)) {
      // Undo, so add<->delete (left<->right)
      res.push({ seq: entry.seq, left: entry.right, right: entry.left })
    }
  }

  return res
}

module.exports = getDiffs
