import axios from 'axios'
import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import {
  FirehoseSubscriptionBase,
  getOpsByType,
  CreateOp,
} from './util/subscription'

async function fetchPost(postID: string) {
  const res = await axios.get(`https://bsky-search.jazco.io/post/${postID}`)
  return res.data
}

async function shouldIncludePost(create: any): Promise<boolean> {
  // lookup post ID at https://bsky-search.jazco.io/post?id={}
  // Wait for 1 second to make sure the post is indexed
  const postID = create.uri.split('/').pop()
  if (postID === undefined) {
    return false
  }

  await new Promise((resolve) => setTimeout(resolve, 1000))

  try {
    const post = await fetchPost(postID)
    if (
      post !== null &&
      post.sentiment !== null &&
      post.sentiment !== undefined &&
      post.sentiment_confidence !== null &&
      post.sentiment_confidence !== undefined
    ) {
      if (post.sentiment.includes('p') && post.sentiment_confidence > 0.65) {
        console.log(`Including post: ${create.uri}`)
        return true
      }
    }
  } catch (e) {
    console.log(
      `Error fetching post ${create.uri}: ${e.response.status} - ${e?.response.data?.error}`,
    )
  }
  return false
}

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    // This logs the text of every post off the firehose.
    // Just for fun :)
    // Delete before actually using
    // for (const post of ops.posts.creates) {
    //   console.log(post.uri.split('/').pop())
    // }

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)

    const positiveFilterMask = await Promise.all(
      ops.posts.creates.map((create) => shouldIncludePost(create)),
    )

    const positivePosts = ops.posts.creates.filter(
      (_, i) => positiveFilterMask[i],
    )

    const hellthreadPosts = ops.posts.creates.filter(
      (create) =>
        create.record.reply?.root.uri?.split('/').pop() === '3juzlwllznd24',
    )

    const postsToCreate = positivePosts.map((create) => {
      return {
        uri: create.uri,
        cid: create.cid,
        replyParent: create.record?.reply?.parent.uri ?? null,
        replyRoot: create.record?.reply?.root.uri ?? null,
        feed: 'positivifeed',
        indexedAt: new Date().toISOString(),
      }
    })

    postsToCreate.push(
      ...hellthreadPosts.map((create) => {
        // map alf-related posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
          feed: 'hellthread',
          indexedAt: new Date().toISOString(),
        }
      }),
    )

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
