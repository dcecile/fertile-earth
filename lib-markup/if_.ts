import type * as markupContext from '@/context.ts'
import * as markupElement from '@/element.ts'
import * as markupFragment from '@/fragment.ts'
import * as markupMarker from '@/marker.ts'
import * as compute from '@symbolize/lib-compute'
import type * as styling from '@symbolize/lib-styling'

type Falsy = '' | 0 | 0n | false | null | undefined

export function if_<CustomContext, Value>(
  ifBranch: (
    value: compute.Computation<Exclude<Value, Falsy>>,
  ) => markupFragment.FragmentOpt<CustomContext>,
  elseBranch: () => markupFragment.FragmentOpt<CustomContext>,
  condition: compute.NodeOpt<Value>,
): markupFragment.Fragment<CustomContext> {
  return new If_(ifBranch, elseBranch, condition)
}

class If_<Value, CustomContext = unknown>
  implements markupFragment.Fragment<CustomContext>
{
  readonly [markupMarker.fragmentMarker]: null = null
  private mutableEndComment: Comment | null = null
  private mutableFragment: markupFragment.Fragment<CustomContext> | null =
    null
  private mutableInnerNodes: readonly Node[] | null = null
  private mutableStartComment: Comment | null = null
  private mutableSub: compute.Computation<void> | null = null

  constructor(
    private readonly ifBranch: (
      value: compute.Computation<Exclude<Value, Falsy>>,
    ) => markupFragment.FragmentOpt<CustomContext>,
    private readonly elseBranch: () => markupFragment.FragmentOpt<CustomContext>,
    private readonly condition: compute.NodeOpt<Value>,
  ) {}

  async add(
    ctx: compute.Context &
      CustomContext &
      markupContext.Context &
      styling.Context,
  ): Promise<void> {
    this.mutableStartComment = ctx.markup.document.createComment('')
    this.mutableEndComment = ctx.markup.document.createComment('')

    let ifResult:
      | [
          compute.Mutation<Exclude<Value, Falsy>>,
          markupFragment.Fragment<CustomContext>,
        ]
      | null = null
    let elseFragment: markupFragment.Fragment<CustomContext> | null = null

    this.mutableSub = await compute.effect(async (value) => {
      if (value) {
        const truthyValue = value as Exclude<Value, Falsy>
        if (elseFragment) {
          await elseFragment.remove()
          elseFragment = null
        }
        if (!ifResult) {
          const ifState = compute.state(truthyValue)
          const ifFragment = markupFragment.toFragment(
            this.ifBranch(ifState),
          )
          ifResult = [ifState, ifFragment]
          const mutableIfNodes: Node[] = []
          await ifFragment.add(ctx)
          if (this.mutableStartComment && this.mutableEndComment) {
            for (const node of ifFragment.nodes()) {
              mutableIfNodes.push(node)
            }
            this.mutableInnerNodes = markupElement.replaceBetween(
              this.mutableStartComment,
              this.mutableEndComment,
              mutableIfNodes,
            )
          }
          this.mutableFragment = ifFragment
        } else {
          const [ifState] = ifResult
          await compute.txn(ctx, async () => {
            await compute.set(ctx, ifState, truthyValue)
          })
        }
      } else {
        if (ifResult) {
          const [, ifFragment] = ifResult
          await ifFragment.remove()
          ifResult = null
        }
        if (!elseFragment) {
          elseFragment = markupFragment.toFragment(this.elseBranch())
          const mutableElseNodes: Node[] = []
          await elseFragment.add(ctx)
          if (this.mutableStartComment && this.mutableEndComment) {
            for (const node of elseFragment.nodes()) {
              mutableElseNodes.push(node)
            }
            this.mutableInnerNodes = markupElement.replaceBetween(
              this.mutableStartComment,
              this.mutableEndComment,
              mutableElseNodes,
            )
          }
          this.mutableFragment = elseFragment
        }
      }
    }, this.condition)
  }

  *nodes(): IterableIterator<Node> {
    if (
      this.mutableStartComment &&
      this.mutableEndComment &&
      this.mutableInnerNodes
    ) {
      yield this.mutableStartComment
      for (const node of this.mutableInnerNodes) {
        yield node
      }
      yield this.mutableEndComment
    }
  }

  async remove(): Promise<void> {
    if (this.mutableFragment) {
      await this.mutableFragment.remove()
      this.mutableFragment = null
    }

    if (this.mutableSub) {
      compute.unsubscribe(this.mutableSub)
      this.mutableSub = null
    }

    this.mutableStartComment = null
    this.mutableEndComment = null
    this.mutableInnerNodes = null
  }
}
