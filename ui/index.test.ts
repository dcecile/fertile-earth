import * as test from '@tiny/test/index.ts'
import type * as widget from '@tiny/ui/widget.ts'
import type * as time from '@tiny/util/time.ts'

export const all: test.TestCollection<widget.Context> = () => [
  import('@fe/ui/button.test.ts'),
  import('@fe/ui/member.test.ts'),
]

export async function run(
  ctx: widget.Context & time.Context
): Promise<boolean> {
  const coreTest = await import('@fe/core/index.test.ts')
  const uiTest = await import('@fe/ui/index.test.ts')
  const tinyUtilTest = await import(
    '@tiny/util/index.test.ts'
  )
  return await test.runAll(ctx, [
    coreTest,
    uiTest,
    tinyUtilTest,
  ])
}
