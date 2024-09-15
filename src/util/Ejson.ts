export function stringify(obj: any): string {
  return JSON.stringify(serialize(obj));
}
export function parse(str: string): any {
  return deserialize(JSON.parse(str));
}
export async function asyncStringify(obj: any): Promise<string> {
  const v = await tickAsync(asyncSerialize, obj);
  if (Array.isArray(v)) { return await asyncJsonStringify(v); }
  return await tickAsync(JSON.stringify, v);
}
export async function asyncParse(str: string): Promise<any> {
  const v = await tickAsync(JSON.parse, str);
  return await tickAsync(asyncDeserialize, v);
}

const LOOP = typeof setImmediate === 'function' ? 1 : 100;
const _setImmediate = (typeof setImmediate === 'function') ? setImmediate : setTimeout;

export function tickAsync(func: (v: any) => any, val: any): Promise<any> {
  return new Promise<any>((resolve) => {
    _setImmediate(() => {
      resolve(Promise.resolve(func(val)))
    });
  })
}

/**
 * Deserialize is not reverse of stringify. Deserialize converts from a plain object to a object for Mongo db.
 * @param val object
 */
export function deserialize(val: any): any {
  if (Array.isArray(val)) { return deconvertArray(val); }
  if (val === null) { return null; }
  if (typeof val === "object") {
    if (val.$date) { return new Date(val.$date); }
    if (val.$undefined) { return null; }
    return deconvertObject(val);
  }
  return val;
}

export async function asyncDeserialize(val: any): Promise<any> {
  if (Array.isArray(val)) { return await asyncDeconvertArray(val); }
  if (val === null) { return null; }
  if (typeof val === "object") {
    if (val.$date) { return new Date(val.$date); }
    if (val.$undefined) { return null; }
    return deconvertObject(val);
  }
  return val;
}

function deconvertArray(val: any[]): any[] {
  const out: any[] = [];
  for (const row of val) {
    out.push(deserialize(row));
  }
  return out;
}

async function asyncDeconvertArray(val: any[]): Promise<any[]> {
  const out: any[] = [];
  let c = 0;
  for (const row of val) {
    if (c > LOOP) {
      c = 0;
      out.push(await tickAsync(asyncDeserialize, row));
    } else {
      c++;
      out.push(await asyncDeserialize(row));
    }
  }
  return out;
}

function deconvertObject(obj: { [key: string]: any }): { [key: string]: any } {
  const out: { [key: string]: any } = {};
  for (const key of Object.keys(obj)) {
    out[key] = deserialize(obj[key]);
  }
  return out;
}

export function serialize(val: any): any {
  if (val instanceof Date) {
    return { $date: val.toISOString() };
  }
  if (Array.isArray(val)) { return convertArray(val); }
  if (val === null) { return null; }
  if (typeof val === "undefined") { return null; }
  if (typeof val === "object") { return convertObject(val); }
  return val;
}

export async function asyncSerialize(val: any): Promise<any> {
  if (val instanceof Date) {
    return { $date: val.toISOString() };
  }
  if (Array.isArray(val)) { return await asyncConvertArray(val); }
  if (val === null) { return null; }
  if (typeof val === "undefined") { return null; }
  if (typeof val === "object") { return convertObject(val); }
  return val;
}

function convertArray(val: any[]): any[] {
  const out: any[] = [];
  for (const row of val) {
    out.push(serialize(row));
  }
  return out;
}

async function asyncConvertArray(val: any[]): Promise<any[]> {
  const out: any[] = [];
  let c = 0;
  for (const row of val) {
    if (c > LOOP) {
      c = 0;
      out.push(await tickAsync(asyncSerialize, row));
    } else {
      c++;
      out.push(await asyncSerialize(row));
    }
  }
  return out;
}

async function asyncJsonStringify(val: any[]): Promise<string> {
  const out: any[] = [];
  let c = 0;
  for (const row of val) {
    if (c > LOOP) {
      c = 0;
      out.push(await tickAsync(JSON.stringify, row));
    } else {
      c++;
      out.push(JSON.stringify(row));
    }
  }
  return '[' + out.join(',') + ']';
}

function convertObject(obj: { [key: string]: any }): { [key: string]: any } {
  const out: { [key: string]: any } = {};
  for (const key of Object.keys(obj)) {
    out[key] = serialize(obj[key]);
  }
  return out;
}
