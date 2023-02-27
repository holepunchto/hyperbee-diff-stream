const toArray = require('stream-to-array')

async function getDiffs (oldBee, newBee, oldIndexedL) {
  // Note: we need to explicitly pass the old indexed length,
  // because even on a snapshot bee the underlying core gets updated

  const newApplyDiff = await toArray(
    newBee.createDiffStream(oldIndexedL)
  )

  // No fork for old bee (note: init indexedL is 0, but init version 1)
  if (oldBee.version === oldIndexedL || oldIndexedL === 0) {
    return newApplyDiff
  }

  throw new Error('not implemented')
}

module.exports = getDiffs
