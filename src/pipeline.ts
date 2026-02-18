import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'
import { log } from 'tiny-typescript-logger'

import { closePool } from './db'
import { extractCustomers } from './extractors/customers'
import { extractOrders } from './extractors/orders'
import { extractEvents } from './extractors/events'
import { buildDatePath, writeParquet } from './loaders/parquet'
import type { StreamExtractResult } from './stream'

interface EtlMetadata {
  counts: {
    customers: number
    events: number
    orders: number
  }
  duration_ms: number
  error?: string
  last_run_at: string
  status: 'success' | 'error'
}

async function uploadMetadata(
  bucket: string,
  metadata: EtlMetadata
): Promise<void> {
  const s3 = new S3Client({ region: process.env.S3_REGION ?? 'us-east-1' })
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: 'etl.json',
      Body: JSON.stringify(metadata, null, 2),
      ContentType: 'application/json'
    })
  )
}

async function extractAndLoad(
  extractor: () => Promise<StreamExtractResult>,
  table: string,
  outputPath: string
): Promise<number> {
  const { stream, getCount, cleanup } = await extractor()

  try {
    await writeParquet(stream, table, outputPath)
    return getCount()
  } finally {
    await cleanup()
  }
}

export async function runPipeline(): Promise<void> {
  const bucket = process.env.S3_BUCKET ?? 'my-data-warehouse'
  const start = Date.now()

  log.info('Starting ETL pipeline...')

  try {
    const now = new Date()

    const [customersCount, ordersCount, eventsCount] = await Promise.all([
      extractAndLoad(
        extractCustomers,
        'customers',
        buildDatePath(bucket, 'customers', now)
      ),
      extractAndLoad(
        extractOrders,
        'orders',
        buildDatePath(bucket, 'orders', now)
      ),
      extractAndLoad(
        extractEvents,
        'events',
        buildDatePath(bucket, 'events', now)
      )
    ])

    log.info(`Streamed ${customersCount} customers`)
    log.info(`Streamed ${ordersCount} orders`)
    log.info(`Streamed ${eventsCount} events`)

    const metadata: EtlMetadata = {
      counts: {
        customers: customersCount,
        orders: ordersCount,
        events: eventsCount
      },
      duration_ms: Date.now() - start,
      last_run_at: now.toISOString(),
      status: 'success'
    }

    await uploadMetadata(bucket, metadata)

    log.info(`Pipeline complete in ${metadata.duration_ms}ms`)
  } catch (error) {
    const metadata: EtlMetadata = {
      last_run_at: new Date().toISOString(),
      status: 'error',
      duration_ms: Date.now() - start,
      counts: { customers: 0, orders: 0, events: 0 },
      error: error instanceof Error ? error.message : String(error)
    }

    await uploadMetadata(bucket, metadata).catch(() => {})

    log.error('Pipeline failed:', error)
    throw error
  } finally {
    await closePool()
  }
}

if (import.meta.main) {
  runPipeline()
}
