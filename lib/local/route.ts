import * as formDataPolyfill from 'formdata-polyfill/esm.min.js'
import type * as nodeBuffer from 'node:buffer'
import type * as nodeHttp from 'node:http'
import * as nodeStream from 'node:stream'
import * as nodeStreamPromises from 'node:stream/promises'
import type * as nodeStreamWeb from 'node:stream/web'
import nodeFetchBody from 'node-fetch/src/body.js'
import type * as typeFest from 'type-fest'

export type Context = {
  maxRequestNonStreamedBytes: number
}

type Request = {
  origin: string
  path: string
  match: Record<string, string>
  params: Record<string, string>
  method: string
  headers: Record<string, string>
  stream(): nodeStreamWeb.ReadableStream
  blob(): Promise<nodeBuffer.Blob>
  buffer(): Promise<ArrayBuffer>
  text(): Promise<string>
  form(): Promise<FormData>
  json(): Promise<typeFest.JsonValue>
}

type Response = typeFest.Promisable<
  | {
      status: number
      headers?: Record<string, string>
      stream?: typeFest.Promisable<nodeStreamWeb.ReadableStream>
      blob?: typeFest.Promisable<nodeBuffer.Blob>
      buffer?: typeFest.Promisable<ArrayBuffer>
      text?: typeFest.Promisable<string>
      form?: typeFest.Promisable<FormData>
      json?: typeFest.Promisable<typeFest.JsonValue>
    }
  | nodeHttp.RequestListener
>

class ResponseError extends Error {
  response: {
    status: number
    headers?: Record<string, string>
    buffer?: ArrayBuffer
    text?: string
    form?: FormData
    json?: typeFest.JsonValue
  }

  constructor(response: ResponseError['response']) {
    super(`Response ${response.status} error`)
    this.response = response
  }
}

type Handler<
  CustomContext,
  CustomRequest = Request,
  CustomResponse = Response,
> = (
  ctx: CustomContext,
  request: CustomRequest
) => CustomResponse

type Route<CustomContext> = {
  methods: string[] | undefined
  match: string | RegExp
  handler: Handler<CustomContext>
}

export function define<CustomContext = unknown>(
  methods: string[] | undefined,
  match: string | RegExp,
  handler: Handler<CustomContext>
): Route<CustomContext> {
  return { methods, match, handler }
}

export function handle<CustomContext>(
  ctx: CustomContext & Context,
  routes: Route<CustomContext>[]
): nodeHttp.RequestListener {
  return (req, res) => {
    void handleRequest(ctx, routes, req, res)
  }
}

async function handleRequest<CustomContext>(
  ctx: CustomContext & Context,
  routes: Route<CustomContext>[],
  req: nodeHttp.IncomingMessage,
  res: nodeHttp.ServerResponse
): Promise<void> {
  if (!req.method) {
    console.error('Missing method')
    return
  }
  if (!req.url) {
    console.error('Missing URL')
    return
  }
  req.on('error', console.error)
  res.on('error', console.error)
  try {
    const url = new URL(
      req.url,
      `http://${req.headers.host || 'localhost'}`
    )
    const checkContentLength = () => {
      if (
        !(
          Number.parseInt(
            req.headers['content-length'] ?? ''
          ) <= ctx.maxRequestNonStreamedBytes
        )
      ) {
        throw new ResponseError({
          status: 400,
          text: `invalid content length (max ${ctx.maxRequestNonStreamedBytes} bytes)`,
        })
      }
    }
    const request: Request = {
      origin: url.origin,
      path: url.pathname,
      params: Object.fromEntries(
        url.searchParams.entries()
      ),
      match: {},
      method: req.method,
      headers: Object.fromEntries(
        Object.entries(req.headers).filter(
          ([_key, value]) => typeof value === 'string'
        ) as [string, string][]
      ),
      stream() {
        return nodeStream.Readable.toWeb(req)
      },
      async blob() {
        checkContentLength()
        return await new nodeFetchBody(req).blob()
      },
      async buffer() {
        checkContentLength()
        return await new nodeFetchBody(req).arrayBuffer()
      },
      async text() {
        checkContentLength()
        return await new nodeFetchBody(req).text()
      },
      async form() {
        checkContentLength()
        return await new nodeFetchBody(req).formData()
      },
      async json() {
        checkContentLength()
        return (await new nodeFetchBody(
          req
        ).json()) as typeFest.JsonValue
      },
    }
    const response = match(ctx, request, routes)
    let headResponse: Awaited<Response>
    try {
      headResponse = await Promise.resolve(response)
    } catch (error) {
      if (error instanceof ResponseError) {
        headResponse = error.response
      } else {
        throw error
      }
    }
    if (typeof headResponse === 'function') {
      headResponse(req, res)
    } else {
      res.writeHead(
        headResponse.status,
        headResponse.headers
      )
      if (headResponse.stream !== undefined) {
        await nodeStreamPromises.pipeline(
          nodeStream.Readable.fromWeb(
            await Promise.resolve(headResponse.stream)
          ),
          res
        )
      } else if (headResponse.blob !== undefined) {
        const blob = await Promise.resolve(
          headResponse.blob
        )
        await nodeStreamPromises.pipeline(
          nodeStream.Readable.fromWeb(blob.stream()),
          res
        )
      } else if (headResponse.buffer !== undefined) {
        res.write(
          await Promise.resolve(headResponse.buffer)
        )
      } else if (headResponse.text !== undefined) {
        res.write(await Promise.resolve(headResponse.text))
      } else if (headResponse.form !== undefined) {
        const result = formDataPolyfill.formDataToBlob(
          await Promise.resolve(headResponse.form)
        )
        res.write(result)
      } else if (headResponse.json !== undefined) {
        const result = JSON.stringify(
          await Promise.resolve(headResponse.json)
        )
        res.write(result)
      }
      res.end()
    }
  } catch (error) {
    console.error(error)
    if (res.headersSent) {
      console.error('Headers already sent')
    } else {
      res.writeHead(500)
    }
    res.end()
  }
}

function match<Context>(
  ctx: Context,
  request: Request,
  routes: Route<Context>[]
): Response {
  let response: Response | undefined = undefined
  for (const route of routes) {
    const match =
      (route.methods === undefined ||
        route.methods.indexOf(request.method) >= 0) &&
      (typeof route.match === 'string'
        ? route.match === request.path && {
            groups: undefined,
          }
        : route.match.exec(request.path))
    if (match) {
      response = route.handler(ctx, {
        ...request,
        match: match.groups ?? {},
      })
      break
    }
  }
  if (!response) {
    throw new Error('No route found')
  }
  return response
}

export function forward<Context>(
  ctx: Context,
  request: Request,
  match: Record<string, string>,
  route: Route<Context>
): Response {
  return route.handler(ctx, {
    ...request,
    match,
  })
}
