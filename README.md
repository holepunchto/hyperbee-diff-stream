# Hyperbee Diff Stream

Get the diff stream between two versions of a hyperbee used in an autobase view.

## Install
`npm i hyperbee-diff-stream`

## Usage
See [./example.js](example.js)
## API

#### `const diffStream = new BeeDiffStream(oldSnapshot, newSnapshot, [options])`

Make a new BeeDiffStream instance.
`oldSnapshot` and `newSnapshot`should be checkouts of the same hyperbee.

The hyperbee should be an autobase view.

The options are passed on to the `sorted-union-stream` constructor.
