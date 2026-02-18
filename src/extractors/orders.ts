import QueryStream from 'pg-query-stream'

import { getPool } from '../db'
import { createCountingTransform } from '../stream'
import type { StreamExtractResult } from '../stream'

export interface Order {
  id: string
  customer_id: string
  amount: number
  currency: string
  status: string
  created_at: string
}

export async function extractOrders(): Promise<StreamExtractResult> {
  const client = await getPool().connect()
  const query = new QueryStream('SELECT * FROM orders')
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
