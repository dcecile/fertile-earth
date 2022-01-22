import * as appEndpointTopic from '@app/core/endpoint/topic.ts'
import type * as appDbQuery from '@app/db/query/index.ts'
import * as appDbQueryTopic from '@app/db/query/topic.ts'
import * as tinyRoute from '@tiny/api/route.ts'
import * as tinyCrypto from '@tiny/core/crypto.node.ts'
import type * as tinyError from '@tiny/core/error.ts'
import * as tinyDbQuery from '@tiny/db/query.ts'

export const create = tinyRoute.define(
  appEndpointTopic.create,
  async (
    ctx: tinyError.Context & appDbQuery.WriteContext,
    request
  ) => {
    const id = tinyCrypto.hash(
      Buffer.from(request.json.requestId, 'hex')
    )
    const memberId = Buffer.from(
      request.json.memberId,
      'hex'
    )
    const { title, slug, content } = request.json
    await tinyDbQuery.retryQuery(
      ctx,
      'topic create',
      appDbQueryTopic.create,
      id,
      memberId,
      title,
      slug,
      content
    )
    return {
      status: 200,
      headers: {
        'content-type': 'application/json',
      },
      json: {
        id: id.toString('hex'),
      },
    }
  }
)

export const list = tinyRoute.define(
  appEndpointTopic.list,
  async (
    ctx: tinyError.Context & appDbQuery.ReadContext,
    _request
  ) => {
    const results = await tinyDbQuery.retryQuery(
      ctx,
      'topic list',
      appDbQueryTopic.list
    )
    return {
      status: 200,
      json: {
        results: results.map((row) => ({
          id: row.id.toString('hex'),
          updatedAt: row.updated_at.getTime(),
          title: row.title,
          slug: row.slug,
          content: row.content,
        })),
      },
    }
  }
)

export const update = tinyRoute.define(
  appEndpointTopic.update,
  async (
    ctx: tinyError.Context & appDbQuery.WriteContext,
    request
  ) => {
    const id = Buffer.from(request.json.id, 'hex')
    const updatedOld = new Date(request.json.updatedOld)
    const { title, slug, content } = request.json
    const result = await tinyDbQuery.retryQuery(
      ctx,
      'topic update',
      appDbQueryTopic.update,
      id,
      updatedOld,
      title,
      slug,
      content
    )
    if (result) {
      return {
        status: 200,
        json: {
          updated: result.updated.getTime(),
        },
      }
    } else {
      throw new tinyRoute.ResponseError({
        status: 404,
      })
    }
  }
)

export const routes = [create, list, update]
