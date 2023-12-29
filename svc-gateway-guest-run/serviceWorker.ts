import * as collection from '@intertwine/lib-collection'
import * as time from '@intertwine/lib-time'
import * as timeBrowser from '@intertwine/lib-time/index.browser.ts'

declare const self: ServiceWorkerGlobalScope
declare const version: bigint
declare const manifest: Record<string, string>

const cacheName = 'code-v1'
const codeIdPrefix = /^\/\.code\/\.id\//
const codePrefix = /\/\.code\//
const mainHtmlPath = 'svc-gateway-guest-run/main.html'

const cachePromise = self.caches.open(cacheName)

function main(): void {
  console.log(version, 'loading', self, manifest)

  self.addEventListener('message', (event) => {
    console.log(version, 'message', event)
  })

  self.addEventListener('install', () => {
    console.log(version, 'install')
    void self.skipWaiting()
  })

  self.addEventListener('activate', () => {
    console.log(version, 'activate')
    void resetClients()
    void prepareCache()
  })

  self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url)
    if (url.origin === self.origin && event.request.method === 'GET') {
      handle(event, url)
    }
  })
}

function handle(event: FetchEvent, url: URL) {
  const path = url.pathname
  const codeIdPath = collection.stripPrefix(path, codeIdPrefix)
  const codePath = codeIdPath ?? collection.stripPrefix(path, codePrefix)
  if (codeIdPath) {
    handleContentById(event, codeIdPath, true)
  } else if (codePath) {
    handleContentByPath(event, codePath, true)
  } else {
    handleContentByPath(event, mainHtmlPath, false)
  }
}

function handleContentById(
  event: FetchEvent,
  contentPath: string,
  sandbox: boolean
) {
  event.respondWith(patchFetchContent(contentPath, sandbox))
}

function handleContentByPath(
  event: FetchEvent,
  path: string,
  sandbox: boolean
) {
  const contentPath = manifest[path] ?? null
  if (!contentPath) {
    console.error(version, 'manifest error', path)
    event.respondWith(
      new Response('Path missing from manifest', { status: 404 })
    )
  } else {
    handleContentById(event, contentPath, sandbox)
  }
}

const fetchContentMemo = collection.memo(
  async (contentPath: string): Promise<Response> => {
    let ok = false
    try {
      const cache = await cachePromise
      const request = new Request(`/.code/.id/${contentPath}`)
      const cacheResponse = await cache.match(request)
      if (cacheResponse) {
        return cacheResponse
      } else {
        console.log(version, 'cache miss', contentPath)
        const response = await fetch(request)
        ok = response.ok
        if (ok) {
          await cache.put(request, response.clone())
        }
        return response
      }
    } finally {
      if (!ok) {
        fetchContentMemo.delete(contentPath)
      }
    }
  }
)

async function patchFetchContent(
  contentPath: string,
  sandbox: boolean
): Promise<Response> {
  const response = (await fetchContentMemo.get(contentPath)).clone()
  const headers = new Headers(response.headers)
  const contentSecurityPolicy = headers.get('content-security-policy')
  if (!sandbox && contentSecurityPolicy) {
    headers.set(
      'content-security-policy',
      contentSecurityPolicy
        .split(';')
        .filter((item) => item !== 'sandbox')
        .join(';')
    )
  }
  return new Response(response.body, {
    headers,
    status: response.status,
    statusText: response.statusText,
  })
}

async function prepareCache(): Promise<void> {
  const cache = await cachePromise
  const ctx = timeBrowser.initContext()

  await time.delay(ctx, 5_000)

  const contentPaths = new Set(Object.values(manifest))

  for (const key of await cache.keys()) {
    const url = new URL(key.url)
    const contentPath = collection.stripPrefix(url.pathname, codeIdPrefix)
    if (
      url.origin !== self.origin ||
      key.method !== 'GET' ||
      !contentPath ||
      !contentPaths.delete(contentPath)
    ) {
      console.log(version, 'evict', key.method, key.url)
      await cache.delete(key)
    }
  }

  for (const contentPath of contentPaths) {
    console.log(version, 'prepare', contentPath)
    await fetchContentMemo.get(contentPath)
  }
}

async function resetClients(): Promise<void> {
  const clients = await self.clients.matchAll({
    includeUncontrolled: true,
  })
  for (const client of clients) {
    if (client instanceof WindowClient) {
      console.log(version, 'reloading window', client.id)
      client.postMessage('reload')
    }
  }
}

main()

export {}