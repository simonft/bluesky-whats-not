import { create } from 'domain'
import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    // This logs the text of every post off the firehose.
    // Just for fun :)
    // Delete before actually using
    for (const post of ops.posts.creates) {
      console.log(post.record.text)
    }

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const likedPosts = ops.likes.creates.map((like) => like.record.subject.uri);
    const postsToCreate = ops.posts.creates.filter((create) => create.record?.reply?.parent.uri === undefined)
      .map((create) => {
        return {
          uri: create.uri,
          cid: create.cid,
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      console.log('creating posts', postsToCreate)
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
    if (likedPosts.length > 0) {
      const toDelete = await this.db
        .selectFrom('post')
        .where('uri', 'in', likedPosts)
        .selectAll()
        .execute()
      const result = await this.db
        .deleteFrom('post')
        .where('uri', 'in', likedPosts)
        .execute()
      if (toDelete.length > 0) {
        console.log(toDelete)
        console.log('deleted posts', result[0].numDeletedRows)
      }
    }
  }
}