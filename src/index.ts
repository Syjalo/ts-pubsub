type TopicMap = Record<string, unknown[]>;
type TopicShape<T> = { [K in keyof T]: unknown[] };
type TopicMessage<T extends TopicShape<T>> = {
  [K in keyof T]: [K, ...T[K]];
}[keyof T];

type Subscriber<Args extends unknown[] = any[]> = ((...args: Args) => void) & {
  original?: (...args: Args) => void;
};

interface SubOptions {
  signal?: AbortSignal;
}

function getAbortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("Aborted", "AbortError");
}

class PubSub<T extends TopicShape<T> = TopicMap> {
  private subscribers = new Map<keyof T, Map<Subscriber, () => void>>();
  private wildcardSubscribers = new Set<(message: TopicMessage<T>) => void>();

  public pub<K extends keyof T>(topic: K, ...args: T[K]): void {
    const subs = this.subscribers.get(topic);
    if (subs) {
      for (const subscriber of [...subs.keys()]) {
        subscriber(...args);
      }
    }

    if (this.wildcardSubscribers.size > 0) {
      const message = [topic, ...args] as TopicMessage<T>;
      for (const subscriber of [...this.wildcardSubscribers]) {
        subscriber(message);
      }
    }
  }

  public sub<K extends keyof T>(
    topic: K,
    options?: SubOptions,
  ): AsyncGenerator<T[K]>;
  public sub<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
    options?: SubOptions,
  ): () => void;
  public sub<K extends keyof T>(
    topic: K,
    subscriberOrOptions?: ((...args: T[K]) => void) | SubOptions,
    options?: SubOptions,
  ): (() => void) | AsyncGenerator<T[K]> {
    if (typeof subscriberOrOptions !== "function") {
      return this.createAsyncIterable<T[K]>(
        (push) => this.sub(topic, (...args) => push(args)),
        subscriberOrOptions?.signal,
      );
    }

    const subscriber = subscriberOrOptions;
    const signal = options?.signal;
    if (signal?.aborted) return () => {};

    let subs = this.subscribers.get(topic);
    if (!subs) {
      subs = new Map();
      this.subscribers.set(topic, subs);
    } else {
      // Detach any prior abort listener if re-subscribing the same reference
      subs.get(subscriber)?.();
    }

    const onAbort = () => this.unsub(topic, subscriber);
    signal?.addEventListener("abort", onAbort, { once: true });
    subs.set(subscriber, () => signal?.removeEventListener("abort", onAbort));

    return () => this.unsub(topic, subscriber);
  }

  public unsub<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
  ): void {
    const subs = this.subscribers.get(topic);
    if (!subs) return;

    this.removeEntry(subs, subscriber);

    for (const item of [...subs.keys()]) {
      if (item.original === subscriber) {
        this.removeEntry(subs, item);
      }
    }

    if (subs.size === 0) {
      this.subscribers.delete(topic);
    }
  }

  public once<K extends keyof T>(
    topic: K,
    options?: SubOptions,
  ): Promise<T[K]>;
  public once<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
    options?: SubOptions,
  ): () => void;
  public once<K extends keyof T>(
    topic: K,
    subscriberOrOptions?: ((...args: T[K]) => void) | SubOptions,
    options?: SubOptions,
  ): Promise<T[K]> | (() => void) {
    if (typeof subscriberOrOptions !== "function") {
      const signal = subscriberOrOptions?.signal;
      if (signal?.aborted) {
        return Promise.reject(getAbortReason(signal));
      }

      const { promise, resolve, reject } = Promise.withResolvers<T[K]>();
      let settled = false;

      const onAbort = () => {
        if (settled) return;
        settled = true;
        unsub();
        reject(getAbortReason(signal!));
      };

      const unsub = this.once(topic, (...args) => {
        if (settled) return;
        settled = true;
        signal?.removeEventListener("abort", onAbort);
        resolve(args);
      });

      signal?.addEventListener("abort", onAbort, { once: true });
      return promise;
    }

    const subscriber = subscriberOrOptions;
    const wrapper: Subscriber<T[K]> = (...args) => {
      unsubscribe();
      subscriber(...args);
    };
    wrapper.original = subscriber;

    const unsubscribe = this.sub(topic, wrapper, options);
    return unsubscribe;
  }

  public clear(topic?: keyof T): void {
    if (topic !== undefined) {
      const subs = this.subscribers.get(topic);
      if (subs) {
        for (const cleanup of subs.values()) cleanup();
        this.subscribers.delete(topic);
      }
      return;
    }

    for (const subs of this.subscribers.values()) {
      for (const cleanup of subs.values()) cleanup();
    }
    this.subscribers.clear();
    this.wildcardSubscribers.clear();
  }

  public count(topic?: keyof T): number {
    if (topic !== undefined) {
      return this.subscribers.get(topic)?.size ?? 0;
    }
    let total = this.wildcardSubscribers.size;
    for (const subs of this.subscribers.values()) {
      total += subs.size;
    }
    return total;
  }

  public topics(): (keyof T)[] {
    return [...this.subscribers.keys()];
  }

  public [Symbol.asyncIterator](): AsyncGenerator<TopicMessage<T>> {
    return this.createAsyncIterable<TopicMessage<T>>((push) =>
      this.subAll(push),
    );
  }

  public readable(): ReadableStream<TopicMessage<T>>;
  public readable<K extends keyof T>(topic: K): ReadableStream<T[K]>;
  public readable<K extends keyof T>(
    topic?: K,
  ): ReadableStream<TopicMessage<T>> | ReadableStream<T[K]> {
    return topic !== undefined
      ? this.createReadable<T[K]>((push) =>
          this.sub(topic, (...args) => push(args)),
        )
      : this.createReadable<TopicMessage<T>>((push) => this.subAll(push));
  }

  public writable(): WritableStream<TopicMessage<T>>;
  public writable<K extends keyof T>(topic: K): WritableStream<T[K]>;
  public writable<K extends keyof T>(
    topic?: K,
  ): WritableStream<TopicMessage<T>> | WritableStream<T[K]> {
    if (topic !== undefined) {
      return new WritableStream<T[K]>({
        write: (args) => {
          this.pub(topic, ...args);
        },
      });
    }

    return new WritableStream<TopicMessage<T>>({
      write: ([topicName, ...args]) => {
        this.pub(topicName, ...(args as any));
      },
    });
  }

  private removeEntry(
    subs: Map<Subscriber, () => void>,
    subscriber: Subscriber,
  ): void {
    const cleanup = subs.get(subscriber);
    if (!cleanup) return;
    cleanup();
    subs.delete(subscriber);
  }

  private subAll(subscriber: (message: TopicMessage<T>) => void): () => void {
    this.wildcardSubscribers.add(subscriber);
    return () => this.wildcardSubscribers.delete(subscriber);
  }

  private createReadable<U>(
    subscribe: (push: (item: U) => void) => () => void,
  ): ReadableStream<U> {
    let unsubscribe = () => {};
    return new ReadableStream<U>({
      start: (controller) => {
        unsubscribe = subscribe((item) => controller.enqueue(item));
      },
      cancel: () => unsubscribe(),
    });
  }

  private createAsyncIterable<U>(
    subscribe: (push: (item: U) => void) => () => void,
    signal?: AbortSignal,
  ): AsyncGenerator<U> {
    if (signal?.aborted) {
      return (async function* () {})();
    }

    let queue: U[] = [];
    let notify: (() => void) | null = null;
    let done = false;

    const wake = () => {
      notify?.();
      notify = null;
    };

    const unsubscribe = subscribe((item) => {
      if (done) return;
      queue.push(item);
      wake();
    });

    const stop = () => {
      if (done) return;
      done = true;
      signal?.removeEventListener("abort", stop);
      unsubscribe();
      wake();
    };

    signal?.addEventListener("abort", stop, { once: true });

    const generator = (async function* () {
      try {
        while (!done) {
          if (queue.length > 0) {
            const batch = queue;
            queue = [];
            for (const item of batch) {
              if (done) return;
              yield item;
            }
            continue;
          }

          const { promise, resolve } = Promise.withResolvers<void>();
          notify = resolve;
          await promise;
        }
      } finally {
        stop();
      }
    })();

    const origReturn = generator.return.bind(generator);
    generator.return = (value) => {
      stop();
      return origReturn(value);
    };

    const origThrow = generator.throw.bind(generator);
    generator.throw = (err) => {
      stop();
      return origThrow(err);
    };

    return generator;
  }
}

export { PubSub };
export type { TopicMap, TopicMessage, SubOptions };
