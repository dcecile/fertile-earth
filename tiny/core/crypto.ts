import sodium from 'sodium-native'

// TODO move to crypto.node

export function hash(input: Buffer): Buffer {
  const output = Buffer.alloc(
    sodium.crypto_generichash_BYTES
  )
  sodium.crypto_generichash(output, input)
  return output
}