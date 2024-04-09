import type * as conveyData from '@/data.ts'
import type * as conveyElementAttrsTest from '@/elementAttrs.test-d.ts'
import type * as conveyElementAttrs from '@/elementAttrs.ts'
import type * as compute from '@intertwine/lib-compute'

export type TestAttrs<
  CustomContext,
  BaseElement extends Element,
> = Required<
  Pick<
    conveyElementAttrs.Attrs<CustomContext, BaseElement>,
    'content' | 'onAdd'
  >
> &
  TestListenerAttrs<BaseElement> &
  TestWritableAttrs<BaseElement>

type TestListenerAttrs<BaseElement extends Element> = {
  readonly [Key in keyof conveyElementAttrs.AllAttrs as Key extends (
    `on${infer Type}`
  ) ?
    Lowercase<Type> extends (
      keyof conveyElementAttrsTest.EventMap<BaseElement>
    ) ?
      Key
    : never
  : never]: Key extends `on${infer Type}` ?
    Lowercase<Type> extends infer LowercaseType ?
      LowercaseType extends (
        keyof conveyElementAttrsTest.EventMap<BaseElement>
      ) ?
        (
          event: conveyElementAttrsTest.EventMap<BaseElement>[LowercaseType],
        ) => Promise<void> | void
      : never
    : never
  : never
}

type TestWritableAttrs<BaseElement extends Element> = {
  readonly [Key in Exclude<
    keyof BaseElement,
    keyof conveyElementAttrsTest.SkippedMap | 'content' | 'onAdd'
  > as Key extends Uppercase<Key extends string ? Key : never> ? never
  : NonNullable<BaseElement[Key]> extends (
    | HTMLCollection
    | NamedNodeMap
    | Node
    | NodeList
    | ((...args: never) => unknown)
  ) ?
    never
  : Key]: Key extends keyof conveyElementAttrsTest.OverrideMap ?
    conveyElementAttrsTest.OverrideMap[Key]
  : compute.ComputationOpt<
      BaseElement[Key] extends boolean ? boolean
      : BaseElement[Key] extends SVGAnimatedLength ?
        conveyData.SvgLengthOpt | null
      : BaseElement[Key] extends SVGAnimatedNumber ? number | null
      : BaseElement[Key] extends SVGAnimatedPreserveAspectRatio ?
        conveyData.SvgPreserveAspectRatioOpt | null
      : BaseElement[Key] extends SVGAnimatedRect ? conveyData.Rect | null
      : BaseElement[Key] extends SVGStringList ? string[] | null
      : BaseElement[Key] | null
    >
}

export type EventMap<BaseElement extends Element> = Readonly<
  BaseElement extends HTMLVideoElement ? HTMLVideoElementEventMap
  : BaseElement extends HTMLMediaElement ? HTMLMediaElementEventMap
  : BaseElement extends HTMLBodyElement ? HTMLBodyElementEventMap
  : BaseElement extends HTMLElement ? HTMLElementEventMap
  : BaseElement extends SVGSVGElement ? SVGSVGElementEventMap
  : BaseElement extends SVGElement ? SVGElementEventMap
  : BaseElement extends MathMLElement ? MathMLElementEventMap
  : never
>

export interface SkippedMap {
  readonly accessKeyLabel: never
  readonly align: never
  readonly attributeStyleMap: never
  readonly ['baseURI']: never
  readonly childElementCount: never
  readonly classList: never
  readonly className: never
  readonly clientHeight: never
  readonly clientLeft: never
  readonly clientTop: never
  readonly clientWidth: never
  readonly currentScale: never
  readonly currentTranslate: never
  readonly dataset: never
  readonly ['innerHTML']: never
  readonly innerText: never
  readonly isConnected: never
  readonly isContentEditable: never
  readonly localName: never
  readonly ['namespaceURI']: never
  readonly nodeName: never
  readonly nodeType: never
  readonly nodeValue: never
  readonly offsetHeight: never
  readonly offsetLeft: never
  readonly offsetTop: never
  readonly offsetWidth: never
  readonly ['outerHTML']: never
  readonly outerText: never
  readonly part: never
  readonly popover: never
  readonly popoverTargetAction: never
  readonly prefix: never
  readonly scrollHeight: never
  readonly scrollLeft: never
  readonly scrollTop: never
  readonly scrollWidth: never
  readonly style: never
  readonly tagName: never
  readonly textContent: never
  readonly transform: never
  readonly validationMessage: never
  readonly validity: never
  readonly willValidate: never
}

export type OverrideMap = Required<
  conveyElementAttrs.PickAttrs<
    | 'ariaAtomic'
    | 'ariaAutoComplete'
    | 'ariaBusy'
    | 'ariaChecked'
    | 'ariaCurrent'
    | 'ariaDisabled'
    | 'ariaExpanded'
    | 'ariaHasPopup'
    | 'ariaHidden'
    | 'ariaLive'
    | 'ariaModal'
    | 'ariaMultiLine'
    | 'ariaMultiSelectable'
    | 'ariaOrientation'
    | 'ariaPressed'
    | 'ariaReadOnly'
    | 'ariaRequired'
    | 'ariaSelected'
    | 'ariaSort'
    | 'autocapitalize'
    | 'contentEditable'
    | 'dir'
    | 'draggable'
    | 'enterKeyHint'
    | 'formEnctype'
    | 'formMethod'
    | 'hidden'
    | 'inputMode'
    | 'nonce'
    | 'spellcheck'
    | 'translate'
  >
>
