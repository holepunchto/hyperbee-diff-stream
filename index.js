const sameObject = require('same-object')

function getChangedKey (diffEntry) {
  const { left, right } = diffEntry
  return left ? left.key : right.key
}

async function shouldAddNewEntry (newEntry, oldBee) {
  const key = getChangedKey(newEntry)
  const oldEntry = await oldBee.get(key)
  // The seqNr does not matter: can change on truncates while value remains same
  // (so we only compare values)
  // Note: all deletion info is also contained in .left, so no need to
  // have a look at .right--we only care about the current state of the newEntry
  // (not where it changed from)
  const newChanged = !sameObject(oldEntry?.value, newEntry.left?.value)

  return newChanged
}

async function getDiffs (oldBee, newBee) {
  const oldIndexedL = oldBee.core.indexedLength
  const oldIndexedBee = oldBee.checkout(oldIndexedL) // Same as newBee.checkout(oldIndexedL)

  const res = []

  for await (const newEntry of newBee.createDiffStream(oldIndexedL)) {
    if (await shouldAddNewEntry(newEntry, oldBee)) {
      res.push(newEntry)
    }
  }

  for await (const entry of oldBee.createDiffStream(oldIndexedL)) {
    const key = getChangedKey(entry)

    const valueOld = (await oldIndexedBee.get(key))
    const valueNew = (await newBee.get(key))
    const shouldAdd = sameObject(valueOld, valueNew) && !sameObject(entry.left, valueNew) // no change
    if (shouldAdd) {
      // Undo, so add<->delete (left<->right)
      res.push({ seq: entry.seq, left: entry.right, right: entry.left })
    }
  }

  return res
}

module.exports = getDiffs
