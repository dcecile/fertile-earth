import * as contrast from '@/index.ts'
import * as compute from '@intertwine/lib-compute'

export const url = import.meta.url

export const tests = {
  ['base'](_ctx: compute.Context): void {
    contrast.background.color(contrast.rgb(255, 0, 0))
  },

  ['null'](_ctx: compute.Context): void {
    contrast.background.color(null)
  },

  ['non-expression marker'](_ctx: compute.Context): void {
    const color = undefined as unknown as contrast.Color

    // @ts-expect-error -- can't put use marker as non-expression
    contrast.background.color(color)
  },

  ['multi'](_ctx: compute.Context): void {
    contrast.background.color([
      contrast.rgb(255, 0, 0),
      contrast.rgb(0, 255, 0),
    ])
    contrast.background.color(contrast.rgb([255, 0], 0, 0))
    contrast.background.color(contrast.rgb([255, [0, 1]], 0, 0))
  },

  ['compute'](_ctx: compute.Context): void {
    contrast.background.color(contrast.rgb(compute.pure(255), 0, 0))
  },

  ['compute in compute'](_ctx: compute.Context): void {
    contrast.background.color(
      // @ts-expect-error -- can't put compute in compute
      contrast.rgb(compute.pure(compute.pure(255)), 0, 0),
    )
  },

  ['multi in compute'](_ctx: compute.Context): void {
    contrast.background.color(
      // @ts-expect-error -- can't put multi in compute
      contrast.rgb(compute.pure([255]), 0, 0),
    )
  },

  ['pseudo'](_ctx: compute.Context): void {
    contrast.background.color(contrast.rgb(contrast.hover(255), 0, 0))
    contrast.background.color(contrast.rgb(0, contrast.hover(255), 0))
  },

  ['pseudo in multi'](_ctx: compute.Context): void {
    contrast.background.color([
      contrast.rgb(255, 0, 0),
      contrast.hover(contrast.rgb(0, 255, 0)),
    ])
  },

  ['pseudo in compute'](_ctx: compute.Context): void {
    contrast.background.color(
      // @ts-expect-error -- can't put pseudo in compute
      contrast.rgb(compute.pure(contrast.hover(255)), 0, 0),
    )
  },
}