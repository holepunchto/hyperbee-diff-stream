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

  return b4a.compare(k1, k2)
}

function decodeEntry (diffEntry, keyEncoding, valueEncoding) {
  if (!diffEntry) return diffEntry
  if (keyEncoding) diffEntry.key = keyEncoding.decode(diffEntry.key)
  if (valueEncoding) diffEntry.value = valueEncoding.decode(diffEntry.value)
  return diffEntry
}

function createUnionMap (keyEncoding, valueEncoding) {
  const decode = diffEntry => decodeEntry(diffEntry, keyEncoding, valueEncoding)
  const filterSameValue = ({ left, right }) => {
    // Diffs are also yielded when the value is the same, but the sequence
    // is not. This filters out that case.
    if (left?.value === right?.value) return null
    return { left, right }
  }

  return function unionMap (undoDiffEntry, applyDiffEntry) {
    if (undoDiffEntry === null) {
      return filterSameValue({
        left: decode(applyDiffEntry.left),
        right: decode(applyDiffEntry.right)
      }
      )
    }
    if (applyDiffEntry === null) {
      // requires undoing, so reverse
      return filterSameValue({
        left: decode(undoDiffEntry.right),
        right: decode(undoDiffEntry.left)
      })
    }

    const haveSameNewValue = areEqual(undoDiffEntry.left, applyDiffEntry.left)

    if (!haveSameNewValue) {
      // apply-entry wins, but the previous state (.right) is not the value
      // at the last indexedLength, since a diffEntry to undo exists for the same key
      // So we yield that to-undo diffEntry's final state as previous state for this change
      return filterSameValue({
        left: decode(applyDiffEntry.left),
        right: decode(undoDiffEntry.left)
      })
    }
    // else: already processed in prev getDiffs, so filter out
    return null
  }
}

function encodeKey (enc, key) {
  return key ? (enc ? enc.encode(key) : key) : null
}

class BeeDiffStream extends Union {
  constructor (leftSnapshot, rightSnapshot, opts = {}) {
    const valueEncoding = opts.valueEncoding ? codecs(opts.valueEncoding) : leftSnapshot.valueEncoding
    const keyEncoding = opts.keyEncoding ? codecs(opts.keyEncoding) : leftSnapshot.keyEncoding

    const gt = encodeKey(keyEncoding, opts.gt)
    const gte = encodeKey(keyEncoding, opts.gte)
    const lt = encodeKey(keyEncoding, opts.lt)
    const lte = encodeKey(keyEncoding, opts.lte)

    // Binary encodings for easier comparison later
    opts = { ...opts, gt, gte, lt, lte, valueEncoding: 'binary', keyEncoding: 'binary' }

    if (leftSnapshot.core.indexedLength === undefined) {
      throw new Error('Incompatible Hypercore version--must have indexedLength property')
    }

    // We know that everything indexed in both snapshots is shared
    const sharedIndexedL = Math.min(
      leftSnapshot.core.indexedLength, rightSnapshot.core.indexedLength
    )

    // TODO: consider optimisation for case where the version of both streams
    // is lower than the sharedIndexedL (in which case only the changes from
    // the oldest version to the newest must be calculated, on the newest stream)
    // --currently it redundantly calcs diffStreams for both and filters out the
    //   shared entries
    const toUndoDiffStream = leftSnapshot.createDiffStream(sharedIndexedL, opts)
    const toApplyDiffStream = rightSnapshot.createDiffStream(sharedIndexedL, opts)

    super(toUndoDiffStream, toApplyDiffStream, {
      compare: unionCompare,
      map: createUnionMap(keyEncoding, valueEncoding)
    })

    this.closeSnapshots = !(opts.closeSnapshots === false)

    this._leftSnapshot = leftSnapshot
    this._rightSnapshot = rightSnapshot
  }

  _destroy (cb) {
    super._destroy((err) => {
      if (!this.closeSnapshots) return cb(err)

      const onclose = () => cb(err)
      Promise.all([this._leftSnapshot.close(), this._rightSnapshot.close()]).then(onclose, cb)
    })
  }
}

module.exports = BeeDiffStream
