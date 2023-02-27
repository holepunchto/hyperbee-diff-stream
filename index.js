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

  const newApplyDiff = []
  for await (const entry of newBee.createDiffStream(oldIndexedL)) {
    newApplyDiff.push(entry)
  }

  const oldToUndoDiff = []
  for await (const entry of oldBee.createDiffStream(oldIndexedL)) {
    oldToUndoDiff.push(entry)
  }

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

  return res
}

module.exports = getDiffs
