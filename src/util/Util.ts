export class Util {

  static promiseWhile<T>(
    data: T,
    condition: (data: T) => boolean,
    action: (data: T) => Promise<T>,
  ): Promise<T> {
    const whilst = (data: T): Promise<T> => {
      return condition(data) ? action(data).then(whilst) : Promise.resolve(data);
    };
    return whilst(data);
  }
}
