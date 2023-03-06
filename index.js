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

function unionCompare (e1, e2) {
  return b4a.compare(getKey(e1), getKey(e2))
}

function conditionalDecoder (encoding) {
  return encoding
    ? entry => encoding.decode(entry)
    : entry => entry
}

function decoderFactory ({ keyEncoding, valueEncoding }) {
  const decodeKey = conditionalDecoder(keyEncoding)
  const decodeValue = conditionalDecoder(valueEncoding)

  return function decode (diffEntry) {
    const res = { left: null, right: null }
    if (diffEntry.left) {
      res.left = {
        seq: diffEntry.left.seq,
        key: decodeKey(diffEntry.left.key),
        value: decodeValue(diffEntry.left.value)
      }
    }
    if (diffEntry.right) {
      res.right = {
        seq: diffEntry.right.seq,
        key: decodeKey(diffEntry.right.key),
        value: decodeValue(diffEntry.right.value)
      }
    }
    return res
  }
}

function unionMapFactory (decoderOpts) {
  const decode = decoderFactory(decoderOpts)

  return function unionMap (oldEntry, newEntry) {
    if (oldEntry === null) return decode(newEntry)
    // Old entries require undoing, so reverse
    if (newEntry === null) {
      return decode({ seq: oldEntry.seq, left: oldEntry.right, right: oldEntry.left })
    }

    const leftEq = areEqual(oldEntry.left, newEntry.left)
    const rightEq = areEqual(oldEntry.right, newEntry.right)
    if (!(leftEq && rightEq)) return decode(rightEq)
    // else: already processed in prev getDiffs, so filter out
    return null
  }
}

class BeeDiffStream extends Union {
  constructor (oldBee, newBee, opts = {}) {
    const oldIndexedL = oldBee.core.indexedLength
    const keyEnc = oldBee.keyEncoding

    let range = {}
    if (keyEnc) {
      range = {
        gt: opts.gt !== undefined ? enc(keyEnc, opts.gt) : undefined,
        gte: opts.gte !== undefined ? enc(keyEnc, opts.gte) : undefined,
        lt: opts.lt !== undefined ? enc(keyEnc, opts.lt) : undefined,
        lte: opts.lte !== undefined ? enc(keyEnc, opts.lte) : undefined
      }
    }

    // Binary encodings for easier comparison later
    const oldDiffStream = oldBee.createDiffStream(oldIndexedL, {
      ...range,
      keyEncoding: 'binary',
      valueEncoding: 'binary'
    })
    const newDiffStream = newBee.createDiffStream(oldIndexedL, {
      ...range,
      keyEncoding: 'binary',
      valueEncoding: 'binary'
    })

    const mapFun = unionMapFactory({
      keyEncoding: oldBee.keyEncoding,
      valueEncoding: oldBee.valueEncoding
    })
    super(oldDiffStream, newDiffStream, {
      compare: unionCompare,
      map: mapFun,
      ...opts
    })
  }
}

function enc (e, v) {
  if (v === undefined || v === null) return null
  if (e !== null) return e.encode(v)
  if (typeof v === 'string') return b4a.from(v)
  return v
}

module.exports = BeeDiffStream
