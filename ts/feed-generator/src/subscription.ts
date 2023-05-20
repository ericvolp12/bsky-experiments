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

async function getClusterAssignmentForDID(did: string) {
  const res = await axios.get(
    `https://bsky-search.jazco.io/users/by_did/${did}/cluster`,
  )
  return res.data
}

interface CVClass {
  box: number[]
  label: string
  confidence: number
}

interface Image {
  cid: string
  post_id: string
  author_did: string
  alt_text?: string
  mime_type: string
  fullsize_url: string
  thumbnail_url: string
  created_at: string // or Date if you prefer
  cv_completed: boolean
  cv_run_at?: string // or Date if you prefer
  cv_classes?: CVClass[]
}

interface Post {
  id: string
  text: string
  parent_post_id?: string
  root_post_id?: string
  author_did: string
  created_at: string // or Date if you prefer
  has_embedded_media: boolean
  parent_relationship?: string // 'r' or 'q' or null
  sentiment?: string
  sentiment_confidence?: number
  images?: Image[]
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

    const enrichedPosts = await Promise.all(
      ops.posts.creates.map(async (create) => {
        let post: Post
        try {
          // Wait for 2.5 seconds to allow images to be processed
          await new Promise((resolve) => setTimeout(resolve, 2500))
          post = await fetchPost(create.uri.split('/').pop()!)
        } catch (e) {
          console.log(
            `Error fetching post ${create.uri}: ${e.response.status} - ${e?.response.data?.error}`,
          )
          return undefined
        }
        try {
          const cluster_assignment = await getClusterAssignmentForDID(
            post.author_did,
          )

          return {
            ...create,
            post,
            cluster: cluster_assignment.cluster_id,
          }
        } catch (e) {
          console.log(
            `Error fetching cluster assignment for ${post.author_did}: ${e.response.status} - ${e?.response.data?.error}`,
          )
          return {
            ...create,
            cluster: undefined,
            post,
          }
        }
      }),
    )

    const positivePosts = enrichedPosts.filter((create) => {
      if (create === undefined) {
        return false
      }

      const post = create.post as Post
      return (
        post.sentiment?.includes('p') &&
        post.sentiment_confidence !== undefined &&
        post?.sentiment_confidence > 0.65
      )
    })

    const hellthreadPosts = enrichedPosts.filter(
      (create) =>
        create !== undefined &&
        create.record.reply?.root.uri?.split('/').pop() === '3juzlwllznd24',
    )

    const postsToCreate: any = []

    postsToCreate.push(
      ...positivePosts.reduce((acc, create) => {
        if (create === undefined) {
          return acc
        }
        return [
          ...acc,
          {
            uri: create.uri,
            cid: create.cid,
            replyParent: create.record?.reply?.parent.uri ?? null,
            replyRoot: create.record?.reply?.root.uri ?? null,
            feed: 'positivifeed',
            indexedAt: new Date().toISOString(),
          },
        ]
      }, []),
    )

    postsToCreate.push(
      ...hellthreadPosts.reduce((acc, create) => {
        if (create === undefined) {
          return acc
        }
        return [
          ...acc,
          {
            uri: create.uri,
            cid: create.cid,
            replyParent: create.record?.reply?.parent.uri ?? null,
            replyRoot: create.record?.reply?.root.uri ?? null,
            feed: 'hellthread',
            indexedAt: new Date().toISOString(),
          },
        ]
      }, []),
    )

    postsToCreate.push(
      ...enrichedPosts.reduce((acc, create) => {
        if (create === undefined) {
          return acc
        }
        if (create.cluster) {
          return [
            ...acc,
            {
              uri: create.uri,
              cid: create.cid,
              replyParent: create.record?.reply?.parent.uri ?? null,
              replyRoot: create.record?.reply?.root.uri ?? null,
              feed: `cluster-${create.cluster}`,
              indexedAt: new Date().toISOString(),
            },
          ]
        }
        return acc
      }, []),
    )

    // Add cat image posts
    postsToCreate.push(
      ...enrichedPosts.reduce((acc, create) => {
        if (create === undefined) {
          return acc
        }
        if (create.post.author_did === 'did:plc:o7eggiasag3efvoc3o7qo3i3') {
          console.log(JSON.stringify(create.post, null, 2))
        }
        if (create.post.images !== undefined && create.post.images.length > 0) {
          let hasCat = false
          for (const image of create.post.images) {
            if (image.cv_classes !== undefined) {
              for (const cv_class of image.cv_classes) {
                if (cv_class.label === 'cat' && cv_class.confidence > 0.5) {
                  hasCat = true
                  break
                }
              }
            }
          }
          if (hasCat) {
            console.log(`Found cat image post: ${create.uri}`)
            return [
              ...acc,
              {
                uri: create.uri,
                cid: create.cid,
                replyParent: create.record?.reply?.parent.uri ?? null,
                replyRoot: create.record?.reply?.root.uri ?? null,
                feed: 'images-cat',
                indexedAt: new Date().toISOString(),
              },
            ]
          }
        }
        return acc
      }, []),
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
