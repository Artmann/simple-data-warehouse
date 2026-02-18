import { Transform } from 'node:stream'
import type { Readable } from 'node:stream'

export interface StreamExtractResult {
  stream: Readable
  getCount: () => number
  cleanup: () => Promise<void>
}

export function createCountingTransform(): {
  transform: Transform
  getCount: () => number
} {
  let count = 0

  const transform = new Transform({
    objectMode: true,
    transform(row, _encoding, callback) {
      count++
      callback(null, row)
    }
  })

  return { transform, getCount: () => count }
}
