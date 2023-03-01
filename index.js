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

async function shouldAddNewEntryNew (newEntry, oldBee) {
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
    const oldCheck = shouldAddNewEntry(newEntry, oldToUndoDiff)
    const newCheck = await shouldAddNewEntryNew(newEntry, oldBee)
    // console.log(newEntry, 'old:', oldCheck, 'new:', newCheck)
    if (newCheck !== oldCheck) throw new Error('new yikes')
    if (newCheck) {
      res.push(newEntry)
    }
  }

  for (const [key, entry] of oldToUndoDiff) {
    const valueOld = (await oldIndexedBee.get(key))
    const valueNew = (await newBee.get(key))
    // console.log('old', valueOld)
    // console.log('new', valueNew)
    // console.log(entry)

    // TODO: simplify
    const newCondition = sameObject(valueOld, valueNew) && !sameObject(entry.left, valueNew) // no change
    const oldCondition = !newApplyDiff.has(key)
    if (newCondition !== oldCondition) throw new Error(`yikes ${valueOld}, ${valueNew}--${newApplyDiff.get(key)}`)
    if (newCondition) {
      // Undo, so add<->delete (left<->right)
      res.push({ seq: entry.seq, left: entry.right, right: entry.left })
    }
  }

  return res
}

module.exports = getDiffs
