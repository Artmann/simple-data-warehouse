import { DuckDBInstance, DuckDBConnection } from '@duckdb/node-api'
import dayjs from 'dayjs'
import { createWriteStream } from 'node:fs'
import { unlink } from 'node:fs/promises'
import { pipeline, Transform } from 'node:stream'
import { promisify } from 'node:util'
import type { Readable } from 'node:stream'

const pipelineAsync = promisify(pipeline)

export function buildDatePath(
  bucket: string,
  table: string,
  date: Date = new Date()
): string {
  const d = dayjs(date)

  return `s3://${bucket}/${table}/${d.format('YYYY')}/${d.format('MM')}/${d.format('DD')}.parquet`
}

async function configureS3(connection: DuckDBConnection): Promise<void> {
  await connection.run('INSTALL httpfs; LOAD httpfs;')
  await connection.run(
    `SET s3_region='${process.env.S3_REGION ?? 'us-east-1'}';`
  )
  await connection.run(
    `SET s3_access_key_id='${process.env.AWS_ACCESS_KEY_ID ?? ''}';`
  )
  await connection.run(
    `SET s3_secret_access_key='${process.env.AWS_SECRET_ACCESS_KEY ?? ''}';`
  )
}

function createNdjsonTransform(): Transform {
  return new Transform({
    objectMode: true,
    writableObjectMode: true,
    readableObjectMode: false,
    transform(row, _encoding, callback) {
      callback(null, JSON.stringify(row) + '\n')
    }
  })
}

export async function writeParquet(
  stream: Readable,
  table: string,
  outputPath: string
): Promise<void> {
  const tmpPath = `/tmp/${table}-${Date.now()}.ndjson`

  await pipelineAsync(
    stream,
    createNdjsonTransform(),
    createWriteStream(tmpPath)
  )

  const instance = await DuckDBInstance.create()
  const connection = await instance.connect()

  try {
    await configureS3(connection)

    await connection.run(
      `COPY (SELECT * FROM read_json_auto('${tmpPath}', ignore_errors=true)) TO '${outputPath}' (FORMAT PARQUET, COMPRESSION ZSTD)`
    )
  } finally {
    await connection.close()
    await unlink(tmpPath).catch(() => {})
  }
}
