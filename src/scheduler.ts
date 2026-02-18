import dayjs from 'dayjs'
import { log } from 'tiny-typescript-logger'

import { runPipeline } from './pipeline'

const targetHour = 3 // 3 AM
const checkIntervalInMiliseconds = 60_000

async function run(): Promise<void> {
  log.info('Scheduler started. Pipeline will run daily at 3 AM.')

  if (process.env.RUN_ON_START === 'true') {
    log.info('RUN_ON_START=true — running pipeline immediately...')

    try {
      await runPipeline()
    } catch (error) {
      log.error('Initial pipeline run failed:', error)
    }
  }

  let lastRunDate = ''

  setInterval(async () => {
    const now = dayjs()
    const today = now.format('YYYY-MM-DD')

    if (now.hour() === targetHour && lastRunDate !== today) {
      lastRunDate = today

      log.info(`Triggering scheduled pipeline run for ${today}`)

      try {
        await runPipeline()
      } catch (error) {
        log.error('Scheduled pipeline run failed:', error)
      }
    }
  }, checkIntervalInMiliseconds)
}

run()
