import * as appRoute from '@fe/api/route/index.ts'
import * as appEndpointMember from '@fe/core/endpoint/member.ts'
import type * as appDbQuery from '@fe/db/query/index.ts'
import * as appDbQueryMember from '@fe/db/query/member.ts'
import * as route from '@tiny/api/route.ts'
import * as crypto from '@tiny/core/crypto.node.ts'
import type * as errorModule from '@tiny/core/error.ts'
import * as dbQuery from '@tiny/db/query.ts'

export const create = route.define(
  appEndpointMember.create,
  async (
    ctx: errorModule.Context & appDbQuery.WriteContext,
    request
  ) => {
    const requestData = await appRoute.checkRequestJson(
      appEndpointMember.create,
      request
    )
    const id = crypto.hash(
      Buffer.from(requestData.requestId, 'hex')
    )
    const { email, handle } = requestData
    await appRoute.checkConflictQuery(
      appEndpointMember.create,
      async () => {
        await dbQuery.retryDbQuery(
          ctx,
          'member create',
          appDbQueryMember.create,
          id,
          email,
          handle
        )
      }
    )
    return appRoute.checkOkResponse(
      appEndpointMember.create,
      {
        id: id.toString('hex'),
      }
    )
  }
)

export const routes = [create]
