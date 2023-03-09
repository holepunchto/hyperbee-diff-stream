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
  if (typeof k1 === 'string') {
    return k1 < k2 ? -1 : k1 > k2 ? 1 : 0
  }

  throw new Error('Only string or buffer supported')
}

function decodeValue (diffEntry, valueEncoding) {
  if (!valueEncoding || !diffEntry) return diffEntry

  diffEntry.value = valueEncoding.decode(diffEntry.value)
  return diffEntry
}

function createUnionMap (valueEncoding) {
  const decode = diffEntry => decodeValue(diffEntry, valueEncoding)

  return function unionMap (undoDiffEntry, applyDiffEntry) {
    if (undoDiffEntry === null) {
      return { left: decode(applyDiffEntry.left), right: decode(applyDiffEntry.right) }
    }
    if (applyDiffEntry === null) {
      // requires undoing, so reverse
      return { left: decode(undoDiffEntry.right), right: decode(undoDiffEntry.left) }
    }

    const haveSameNewValue = areEqual(undoDiffEntry.left, applyDiffEntry.left)

    if (!haveSameNewValue) {
      // apply-entry wins, but the previous state (.right) is not the value
      // at the last indexedLength, since a diffEntry to undo exists for the same key
      // So we yield that to-undo diffEntry's final state as previous state for this change
      return { left: decode(applyDiffEntry.left), right: decode(undoDiffEntry.left) }
    }
    // else: already processed in prev getDiffs, so filter out
    return null
  }
}

class BeeDiffStream extends Union {
  constructor (leftSnapshot, rightSnapshot, opts = {}) {
    const valueEncoding = opts.valueEncoding ? codecs(opts.valueEncoding) : leftSnapshot.valueEncoding
    // Binary valueEncoding for easier comparison later
    opts = { ...opts, valueEncoding: 'binary' }

    // We know that everything indexed in both snapshots is shared
    const sharedIndexedL = leftSnapshot.core.isAutobase
      ? Math.min(leftSnapshot.core.indexedLength, rightSnapshot.core.indexedLength)
      : leftSnapshot.version // A normal bee doesn't have indexedLength (becomes a normal diffStream)

    const toUndoDiffStream = leftSnapshot.createDiffStream(sharedIndexedL, opts)
    const toApplyDiffStream = rightSnapshot.createDiffStream(sharedIndexedL, opts)

    super(toUndoDiffStream, toApplyDiffStream, {
      compare: unionCompare,
      map: createUnionMap(valueEncoding)
    })

    this._leftSnapshot = leftSnapshot
    this._rightSnapshot = rightSnapshot
  }

  _destroy (cb) {
    super._destroy((err) => {
      const onclose = () => cb(err)
      Promise.all([this._leftSnapshot.close(), this._rightSnapshot.close()]).then(onclose, cb)
    })
  }
}

module.exports = BeeDiffStream
