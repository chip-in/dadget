export function stringify(obj: object): string {
  return JSON.stringify(serialize(obj))
}
export function parse(str: string): any {
  return deserialize(JSON.parse(str))

}
export function deserialize(val: any): any {
  if (Array.isArray(val)) return deconvertArray(val)
  if (val === null) return null
  if (typeof val === "object") {
    if (val.$date) return new Date(val.$date)
    if (val.$undefined) return undefined
    return deconvertObject(val)
  }
  return val

}

function deconvertArray(val: any[]): any[] {
  let out: any[] = []
  for (let i = 0; i < val.length; i++) {
    out.push(deserialize(val[i]))
  }
  return out
}

function deconvertObject(obj: { [key: string]: any }): { [key: string]: any } {
  let out: { [key: string]: any } = {}
  for (let key in obj) {
    out[key] = deserialize(obj[key])
  }
  return out
}

function serialize(val: any): any {
  if (val instanceof Date) {
    return { $date: val.toISOString() }
  }
  if (Array.isArray(val)) return convertArray(val)
  if (val === null) return null
  if (typeof val === "undefined") return { "$undefined": true }
  if (typeof val === "object") return convertObject(val)
  return val
}

function convertArray(val: any[]): any[] {
  let out: any[] = []
  for (let i = 0; i < val.length; i++) {
    out.push(serialize(val[i]))
  }
  return out
}

function convertObject(obj: { [key: string]: any }): { [key: string]: any } {
  let out: { [key: string]: any } = {}
  for (let key in obj) {
    out[key] = serialize(obj[key])
  }
  return out
}
