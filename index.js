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
    if (shouldAddNewEntry(newEntry, oldToUndoDiff)) {
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
