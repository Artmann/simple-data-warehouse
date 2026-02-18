import QueryStream from 'pg-query-stream'

import { getPool } from '../db'
import { createCountingTransform } from '../stream'
import type { StreamExtractResult } from '../stream'

export interface Customer {
  id: string
  email: string
  name: string
  created_at: string
  metadata: Record<string, unknown>
}

export async function extractCustomers(): Promise<StreamExtractResult> {
  const client = await getPool().connect()
  const query = new QueryStream('SELECT * FROM customers')
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
