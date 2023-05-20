import { InvalidRequestError } from '@atproto/xrpc-server'
import { Server } from './lexicon'
import { AppContext } from './config'
import { ClusterMap } from './util/clusters'
import { validateAuth } from './auth'

const availableFeeds = ['hellthread', 'positivifeed', 'cluster', 'images-cat']

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.getFeedSkeleton(async ({ params, req }) => {
    if (
      !params.feed.startsWith(
        'at://did:web:feedsky.jazco.io/app.bsky.feed.generator',
      ) ||
      (!availableFeeds.includes(params.feed.split('/').pop() ?? '') &&
        !params.feed.split('/').pop()?.startsWith('cluster'))
    ) {
      throw new InvalidRequestError(
        'Unsupported algorithm',
        'UnsupportedAlgorithm',
      )
    }
    /**
     * Example of how to check auth if giving user-specific results:
     **/
    // const requesterDid = await validateAuth(
    //   req,
    //   ctx.cfg.serviceDid,
    //   ctx.didResolver,
    // )

    let selectedFeed = params.feed.split('/').pop() ?? ''

    if (selectedFeed.startsWith('cluster-')) {
      const clusterAlias = selectedFeed.substring(8)
      if (!clusterAlias) {
        throw new InvalidRequestError('malformed cluster feed')
      }
      if (!ClusterMap[clusterAlias]) {
        throw new InvalidRequestError('unknown cluster')
      }
      selectedFeed = `cluster-${ClusterMap[clusterAlias].ClusterID}`
    }

    let builder = ctx.db
      .selectFrom('post')
      .where('post.feed', '=', selectedFeed)
      .selectAll()
      .orderBy('indexedAt', 'desc')
      .orderBy('cid', 'desc')
      .limit(params.limit)

    if (params.cursor) {
      const [indexedAt, cid] = params.cursor.split('::')
      if (!indexedAt || !cid) {
        throw new InvalidRequestError('malformed cursor')
      }
      const timeStr = new Date(parseInt(indexedAt, 10)).toISOString()
      builder = builder
        .where('post.indexedAt', '<', timeStr)
        .orWhere((qb) => qb.where('post.indexedAt', '=', timeStr))
        .where('post.cid', '<', cid)
    }
    const res = await builder.execute()

    const feed = res.map((row) => ({
      post: row.uri,
      reply: row.replyParent || null,
    }))

    let cursor: string | undefined
    const last = res.at(-1)
    if (last) {
      cursor = `${new Date(last.indexedAt).getTime()}::${last.cid}`
    }

    return {
      encoding: 'application/json',
      body: {
        cursor,
        feed,
      },
    }
  })
}
