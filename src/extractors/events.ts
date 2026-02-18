import QueryStream from 'pg-query-stream'

import { getPool } from '../db'
import { createCountingTransform } from '../stream'
import type { StreamExtractResult } from '../stream'

export interface Event {
  id: string
  customer_id: string
  event_name: string
  properties: Record<string, unknown>
  created_at: string
}

export async function extractEvents(): Promise<StreamExtractResult> {
  const client = await getPool().connect()
  const query = new QueryStream('SELECT * FROM events')
  const pgStream = client.query(query)

  const { transform, getCount } = createCountingTransform()
  const stream = pgStream.pipe(transform)

  return {
    stream,
    getCount,
    cleanup: async () => {
      client.release()
    }
  }
}
