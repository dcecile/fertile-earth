import * as test from '@tiny/test/index.ts'
import * as errorModule from '@tiny/util/error.ts'
import * as time from '@tiny/util/time.ts'

export const url = import.meta.url

export const tests = {
  async ['one attempt, pass'](
    baseContext: test.TestContext
  ): Promise<void> {
    const ctx: test.TestContext &
      errorModule.RetryContext = {
      ...baseContext,
      random: test.mock([]),
    }
    const expectedResult = {}
    const onError = test.mock([])
    const actualResult = test.sync(
      errorModule.retry(
        ctx,
        () => Promise.resolve(expectedResult),
        {
          minDelayMs: time.interval({ milliseconds: 10 }),
          windowMs: time.interval({ seconds: 10 }),
          maxAttempts: 10,
          onError,
        }
      )
    )
    await ctx.clock.tickAsync(0)
    test.assertEquals(
      actualResult.resolvedValue,
      expectedResult
    )
  },
  async ['two attempts, count limit'](
    baseContext: test.TestContext
  ): Promise<void> {
    const ctx: test.TestContext &
      errorModule.RetryContext = {
      ...baseContext,
      random: test.mock([() => 0.5]),
    }
    const expectedResult = new Error('TEST')
    const onError = test.mock([() => undefined])
    const actualResult = test.sync(
      errorModule.retry(
        ctx,
        () => Promise.reject(expectedResult),
        {
          minDelayMs: time.interval({ milliseconds: 10 }),
          windowMs: time.interval({ seconds: 10 }),
          maxAttempts: 2,
          onError,
        }
      )
    )
    await ctx.clock.tickAsync(0)
    test.assertDeepEquals(onError[test.mockHistory], [
      [expectedResult, 0, 10],
    ])
    test.assertEquals(actualResult.isSettled, false)
    await ctx.clock.tickAsync(10)
    test.assertEquals(
      actualResult.rejectedValue,
      expectedResult
    )
  },
  async ['three attempts, count limit'](
    baseContext: test.TestContext
  ): Promise<void> {
    const ctx: test.TestContext &
      errorModule.RetryContext = {
      ...baseContext,
      random: test.mock([() => 0.8, () => 0.8]),
    }
    const expectedResult = new Error('TEST')
    const onError = test.mock([
      () => undefined,
      () => undefined,
    ])
    const actualResult = test.sync(
      errorModule.retry(
        ctx,
        () => Promise.reject(expectedResult),
        {
          minDelayMs: time.interval({ milliseconds: 10 }),
          windowMs: time.interval({ seconds: 10 }),
          maxAttempts: 3,
          onError,
        }
      )
    )
    await ctx.clock.tickAsync(0)
    test.assertDeepEquals(onError[test.mockHistory], [
      [expectedResult, 0, 10],
    ])
    test.assertEquals(actualResult.isSettled, false)
    await ctx.clock.tickAsync(10)
    test.assertDeepEquals(onError[test.mockHistory], [
      [expectedResult, 0, 10],
      [expectedResult, 1, 16],
    ])
    test.assertEquals(actualResult.isSettled, false)
    await ctx.clock.tickAsync(16)
    test.assertEquals(
      actualResult.rejectedValue,
      expectedResult
    )
  },
  async ['one attempt, window limit'](
    baseContext: test.TestContext
  ): Promise<void> {
    const ctx: test.TestContext &
      errorModule.RetryContext = {
      ...baseContext,
      random: test.mock([() => 0]),
    }
    const expectedResult = new Error('TEST')
    const onError = test.mock([])
    const actualResult = test.sync(
      errorModule.retry(
        ctx,
        async () => {
          await time.delay(
            ctx,
            time.interval({ seconds: 9 })
          )
          throw expectedResult
        },
        {
          minDelayMs: time.interval({ seconds: 1 }),
          windowMs: time.interval({ seconds: 10 }),
          maxAttempts: 10,
          onError,
        }
      )
    )
    await ctx.clock.tickAsync(0)
    test.assertEquals(actualResult.isSettled, false)
    await ctx.clock.tickAsync(9_000)
    test.assertEquals(
      actualResult.rejectedValue,
      expectedResult
    )
  },
  async ['three attempts, window limit'](
    baseContext: test.TestContext
  ): Promise<void> {
    const ctx: test.TestContext &
      errorModule.RetryContext = {
      ...baseContext,
      random: test.mock([() => 0.6, () => 0.6, () => 0]),
    }
    const expectedResult = new Error('TEST')
    const onError = test.mock([
      () => undefined,
      () => undefined,
    ])
    const actualResult = test.sync(
      errorModule.retry(
        ctx,
        () => Promise.reject(expectedResult),
        {
          minDelayMs: time.interval({ seconds: 1 }),
          windowMs: time.interval({
            seconds: 2,
            milliseconds: 201,
          }),
          maxAttempts: 10,
          onError,
        }
      )
    )
    await ctx.clock.tickAsync(0)
    test.assertDeepEquals(onError[test.mockHistory], [
      [expectedResult, 0, 1000],
    ])
    test.assertEquals(actualResult.isSettled, false)
    await ctx.clock.tickAsync(1000)
    test.assertDeepEquals(onError[test.mockHistory], [
      [expectedResult, 0, 1000],
      [expectedResult, 1, 1200],
    ])
    test.assertEquals(actualResult.isSettled, false)
    await ctx.clock.tickAsync(1200)
    test.assertEquals(
      actualResult.rejectedValue,
      expectedResult
    )
  },
  async ['three attempts, pass'](
    baseContext: test.TestContext
  ): Promise<void> {
    const ctx: test.TestContext &
      errorModule.RetryContext = {
      ...baseContext,
      random: test.mock([() => 0.8, () => 0.8]),
    }
    const expectedResult = {}
    const expectedError = new Error('TEST')
    const callback = test.mock([
      () => Promise.reject(expectedError),
      () => Promise.reject(expectedError),
      () => Promise.resolve(expectedResult),
    ])
    const onError = test.mock([
      () => undefined,
      () => undefined,
    ])
    const actualResult = test.sync(
      errorModule.retry(ctx, callback, {
        minDelayMs: time.interval({ milliseconds: 10 }),
        windowMs: time.interval({ seconds: 10 }),
        maxAttempts: 3,
        onError,
      })
    )
    await ctx.clock.tickAsync(0)
    test.assertDeepEquals(onError[test.mockHistory], [
      [expectedError, 0, 10],
    ])
    test.assertEquals(actualResult.isSettled, false)
    await ctx.clock.tickAsync(10)
    test.assertDeepEquals(onError[test.mockHistory], [
      [expectedError, 0, 10],
      [expectedError, 1, 16],
    ])
    test.assertEquals(actualResult.isSettled, false)
    await ctx.clock.tickAsync(16)
    test.assertEquals(
      actualResult.resolvedValue,
      expectedResult
    )
  },
}
