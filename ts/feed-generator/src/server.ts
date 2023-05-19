import { DidResolver, MemoryCache } from '@atproto/did-resolver'
import events from 'events'
import express from 'express'
import promMid from 'express-prometheus-middleware'
import http from 'http'
import morgan from 'morgan'
import client from 'prom-client'
import { AppContext, Config } from './config'
import { Database, createDb, migrateToLatest } from './db'
import feedGeneration from './feed-generation'
import { createServer } from './lexicon'
import { FirehoseSubscription } from './subscription'
import wellKnown from './well-known'

export class FeedGenerator {
  public app: express.Application
  public server?: http.Server
  public db: Database
  public firehose: FirehoseSubscription
  public cfg: Config

  constructor(
    app: express.Application,
    db: Database,
    firehose: FirehoseSubscription,
    cfg: Config,
  ) {
    this.app = app
    this.db = db
    this.firehose = firehose
    this.cfg = cfg
  }

  static create(config?: Partial<Config>) {
    const cfg: Config = {
      port: config?.port ?? 3000,
      hostname: config?.hostname ?? 'feed-generator.test',
      sqliteLocation: config?.sqliteLocation ?? ':memory:',
      subscriptionEndpoint: config?.subscriptionEndpoint ?? 'wss://bsky.social',
      serviceDid: config?.serviceDid ?? 'did:example:test',
    }
    const app = express()

    // Configure Middleware for Telemetry
    app.use(
      promMid({
        metricsPath: '/metrics',
        collectDefaultMetrics: true,
        requestDurationBuckets: [0.001, 0.05, 0.5, 1, 3, 5, 10],
        requestLengthBuckets: client.exponentialBuckets(10, 10, 7),
        responseLengthBuckets: client.exponentialBuckets(10, 10, 7),
        customLabels: ['feed'],
        transformLabels: (labels, req) => {
          let feed = ''
          if (req.query.feed) {
            const fullFeed = req.query.feed as string
            feed = fullFeed.split('/').pop() as string
          }
          labels.feed = feed
        },
      }),
    )

    // Add morgan logging middleware
    app.use(morgan('combined'))

    const db = createDb(cfg.sqliteLocation)
    const firehose = new FirehoseSubscription(db, cfg.subscriptionEndpoint)

    const didCache = new MemoryCache()
    const didResolver = new DidResolver(
      { plcUrl: 'https://plc.directory' },
      didCache,
    )

    const server = createServer({
      validateResponse: true,
      payload: {
        jsonLimit: 100 * 1024, // 100kb
        textLimit: 100 * 1024, // 100kb
        blobLimit: 5 * 1024 * 1024, // 5mb
      },
    })
    const ctx: AppContext = {
      db,
      didResolver,
      cfg,
    }
    feedGeneration(server, ctx)

    app.use(server.xrpc.router)
    app.use(wellKnown(cfg.hostname))

    return new FeedGenerator(app, db, firehose, cfg)
  }

  async start(): Promise<http.Server> {
    await migrateToLatest(this.db)
    this.firehose.run()
    this.server = this.app.listen(this.cfg.port)
    await events.once(this.server, 'listening')
    return this.server
  }
}

export default FeedGenerator
