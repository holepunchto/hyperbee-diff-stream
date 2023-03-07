# Hyperbee Diff Stream

Get the diff stream between two snapshots of a hyperbee. Ideal for autobase views.

For example, when reconnecting after having worked locally for a while,
it can get the diff stream between a snapshot from just before and from just after the autobase linearisation.
Even if several peers (including you in your local fork) made a lot of changes to a particular key, the diff stream will still yield only a single change for it.

## Install
`npm i hyperbee-diff-stream`

## Usage
See [example.js](./example.js)
## API

#### `const diffStream = new BeeDiffStream(leftSnapshot, rightSnapshot, [options])`

Make a new `BeeDiffStream` instance, which is the stream of changes to get from the state of `leftSnapshot` to that of `rightSnapshot`.

The changes are ordered by key.

`leftSnapshot` and `rightSnapshot` should be snapshots (or checkouts) of the same hyperbee. The hyperbee can be an autobase view.

The passed snapshots are managed by `BeeDiffStream`, and will be closed when the diff stream closes.

The `opts` include:

```
{
  gt: 'only consider keys > than this',
  gte: 'only consider keys >= than this',
  lt: 'only consider keys < than this',
  lte: 'only consider keys <= than this',
  keyEncoding: 'utf-8', // a key encoding
  valueEncoding: 'json' // a value encoding
}
```
By default the key- and value encoding of the hyperbee are used.

The stream yields values which have the same format as a Hyperbee diff stream:

```
{
   left: { seq, key, value },
   right: { seq, key, value }
}
```

`left` is the current entry, `right` is the previous entry.

- if `right` is null, the key was added
- If `left` is null, the key was deleted
- If both `left` and `right` are present, the key was updated from `right` to `left`.

Note: when used on an autobase view, it might be tricky to interpret the `seq` value,
since it can refer to either the left or the right snapshot.
(With autobase views, blocks can get removed due to truncates, so the same `seq` might refer to different entries on different snapshots.)
