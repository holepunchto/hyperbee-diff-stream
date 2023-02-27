const toArray = require('stream-to-array')

async function getDiffs (oldBee, newBee, oldIndexedL) {
  // Note: we need to explicitly pass the old indexed length,
  // because even on a snapshot bee the underlying core gets updated
  // console.log(newBase.view.core)
  const newIndexL = newBee.core.indexedLength

  const newApplyDiff = await toArray(
    newBee.createDiffStream(oldIndexedL)
  )
  if (newIndexL === newBee.version) { // No forks
    return newApplyDiff
  }

  return []
}

module.exports = getDiffs
