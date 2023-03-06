const Union = require('sorted-union-stream')
const b4a = require('b4a')
const codecs = require('codecs')

function getKey (diffEntry) {
  const { left, right } = diffEntry
  return left ? left.key : right.key
}

function areEqual (diff1, diff2) {
  if (diff1 === null && diff2 === null) return true
  if (diff1 === null || diff2 === null) return false
  return b4a.equals(diff1.value, diff2.value)
}

function unionCompare (e1, e2) {
  const k1 = getKey(e1)
  const k2 = getKey(e2)

  if (b4a.isBuffer(k1)) return b4a.compare(k1, k2)
  else if (typeof k1 === 'string') {
    if (k1 > k2) return 1
    return k1 < k2 ? -1 : 0
  }

  throw new Error('Only string or buffer supported')
}

function decodeValue (diffEntry, valueEncoding) {
  if (!valueEncoding) return diffEntry

  if (diffEntry.left) diffEntry.left.value = valueEncoding.decode(diffEntry.left.value)
  if (diffEntry.right) diffEntry.right.value = valueEncoding.decode(diffEntry.right.value)

  return diffEntry
}

function unionMapFactory (valueEncoding) {
  const decode = diffEntry => decodeValue(diffEntry, valueEncoding)

  return function unionMap (oldEntry, newEntry) {
    if (oldEntry === null) return decode(newEntry)
    if (newEntry === null) {
      // Old entries require undoing, so reverse
      return decode({ left: oldEntry.right, right: oldEntry.left })
    }

    const leftEq = areEqual(oldEntry.left, newEntry.left)
    const rightEq = areEqual(oldEntry.right, newEntry.right)
    if (!(leftEq && rightEq)) return decode(newEntry) // newest wins
    // else: already processed in prev getDiffs, so filter out
    return null
  }
}

class BeeDiffStream extends Union {
  constructor (oldBee, newBee, opts = {}) {
    const valueEncoding = opts.valueEncoding ? codecs(opts.valueEncoding) : oldBee.valueEncoding
    opts = { ...opts, valueEncoding: undefined }

    // A normal bee doesn't have indexedLength.
    // In this case, we fallback to the version,
    // and the result is a normal diffStream
    const oldIndexedL = oldBee.core.isAutobase ? oldBee.core.indexedLength : oldBee.version

    // Binary encodings for easier comparison later
    oldBee = oldBee.snapshot({ keyEncoding: opts.keyEncoding, valueEncoding: 'binary' })
    newBee = newBee.snapshot({ keyEncoding: opts.keyEncoding, valueEncoding: 'binary' })

    const oldDiffStream = oldBee.createDiffStream(oldIndexedL, opts)
    const newDiffStream = newBee.createDiffStream(oldIndexedL, opts)

    super(oldDiffStream, newDiffStream, {
      compare: unionCompare,
      map: unionMapFactory(valueEncoding),
      ...opts
    })
  }
}

module.exports = BeeDiffStream
