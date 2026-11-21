export type TopicMap = Record<string, unknown[]>;
export type TopicShape<T> = { [K in keyof T]: unknown[] };
export type TopicMessage<T extends TopicShape<T>> = {
  [K in keyof T]: [K, ...T[K]];
}[keyof T];

export interface SubOptions<Args extends unknown[] = unknown[]> {
  signal?: AbortSignal;
  filter?: (...args: Args) => boolean;
}

export interface CollectOptions<Args extends unknown[] = unknown[]>
  extends SubOptions<Args> {
  max?: number;
  time?: number;
  idle?: number;
}

interface TopicSubscription {
  subscriber: unknown;
  active: boolean;
  removeAbortListener: () => void;
  dispatch: (args: unknown[]) => void;
}

function getAbortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("Aborted", "AbortError");
}

export class PubSub<T extends TopicShape<T> = TopicMap> {
  private topicSubscriptions = new Map<
    keyof T,
    Map<unknown, TopicSubscription>
  >();
  private wildcardSubscribers = new Set<(message: TopicMessage<T>) => void>();

  public pub<K extends keyof T>(topic: K, ...args: T[K]): void {
    const subscriptions = this.topicSubscriptions.get(topic);
    if (subscriptions) {
      for (const subscription of [...subscriptions.values()]) {
        if (subscription.active) {
          subscription.dispatch(args);
        }
      }
    }

    if (this.wildcardSubscribers.size > 0) {
      const message = [topic, ...args] as TopicMessage<T>;
      for (const subscriber of [...this.wildcardSubscribers]) {
        if (this.wildcardSubscribers.has(subscriber)) {
          subscriber(message);
        }
      }
    }
  }

  public sub<K extends keyof T>(
    topic: K,
    options?: SubOptions<T[K]>,
  ): AsyncGenerator<T[K]>;
  public sub<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
    options?: SubOptions<T[K]>,
  ): () => void;
  public sub<K extends keyof T>(
    topic: K,
    subscriberOrOptions?: ((...args: T[K]) => void) | SubOptions<T[K]>,
    options?: SubOptions<T[K]>,
  ): (() => void) | AsyncGenerator<T[K]> {
    if (typeof subscriberOrOptions !== "function") {
      return this.createAsyncGenerator<T[K]>(
        (push) =>
          this.sub(topic, (...args) => push(args), {
            filter: subscriberOrOptions?.filter,
          }),
        subscriberOrOptions?.signal,
      );
    }

    return this.addSubscription(topic, subscriberOrOptions, options);
  }

  public unsub<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
  ): void {
    const subscriptions = this.topicSubscriptions.get(topic);
    if (!subscriptions) return;

    for (const [key, subscription] of [...subscriptions.entries()]) {
      if (subscription.subscriber === subscriber) {
        this.removeSubscription(topic, subscriptions, key, subscription);
      }
    }
  }

  public once<K extends keyof T>(
    topic: K,
    options?: SubOptions<T[K]>,
  ): Promise<T[K]>;
  public once<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
    options?: SubOptions<T[K]>,
  ): () => void;
  public once<K extends keyof T>(
    topic: K,
    subscriberOrOptions?: ((...args: T[K]) => void) | SubOptions<T[K]>,
    options?: SubOptions<T[K]>,
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
        unsubscribe();
        reject(getAbortReason(signal!));
      };

      const unsubscribe = this.addSubscription(
        topic,
        (...args) => {
          if (settled) return;
          settled = true;
          signal?.removeEventListener("abort", onAbort);
          resolve(args);
        },
        { filter: subscriberOrOptions?.filter, once: true },
      );

      signal?.addEventListener("abort", onAbort, { once: true });
      return promise;
    }

    return this.addSubscription(topic, subscriberOrOptions, {
      ...options,
      once: true,
    });
  }

  public clear(topic?: keyof T): void {
    if (topic !== undefined) {
      const subscriptions = this.topicSubscriptions.get(topic);
      if (subscriptions) {
        this.clearTopicSubscriptions(subscriptions);
        this.topicSubscriptions.delete(topic);
      }
      return;
    }

    for (const subscriptions of this.topicSubscriptions.values()) {
      this.clearTopicSubscriptions(subscriptions);
    }
    this.topicSubscriptions.clear();
    this.wildcardSubscribers.clear();
  }

  public count(topic?: keyof T): number {
    if (topic !== undefined) {
      return this.topicSubscriptions.get(topic)?.size ?? 0;
    }
    let total = this.wildcardSubscribers.size;
    for (const subscriptions of this.topicSubscriptions.values()) {
      total += subscriptions.size;
    }
    return total;
  }

  public topics(): (keyof T)[] {
    return [...this.topicSubscriptions.keys()];
  }

  public [Symbol.asyncIterator](): AsyncGenerator<TopicMessage<T>> {
    return this.createAsyncGenerator<TopicMessage<T>>((push) =>
      this.addWildcardSubscriber(push),
    );
  }

  public readable(): ReadableStream<TopicMessage<T>>;
  public readable<K extends keyof T>(topic: K): ReadableStream<T[K]>;
  public readable<K extends keyof T>(
    topic?: K,
  ): ReadableStream<TopicMessage<T>> | ReadableStream<T[K]> {
    return topic !== undefined
      ? this.createReadableStream<T[K]>((push) =>
          this.sub(topic, (...args) => push(args)),
        )
      : this.createReadableStream<TopicMessage<T>>((push) =>
          this.addWildcardSubscriber(push),
        );
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
      write: ([messageTopic, ...args]) => {
        this.pub(messageTopic, ...(args as T[typeof messageTopic]));
      },
    });
  }

  private addSubscription<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
    options?: SubOptions<T[K]> & { once?: boolean },
  ): () => void {
    const signal = options?.signal;
    if (signal?.aborted) return () => {};

    const filter = options?.filter;
    const once = options?.once ?? false;
    const key: unknown = filter || once ? Symbol() : subscriber;

    let subscriptions = this.topicSubscriptions.get(topic);
    if (!subscriptions) {
      subscriptions = new Map();
      this.topicSubscriptions.set(topic, subscriptions);
    } else if (key === subscriber) {
      // Replace any prior registration for the same bare subscriber
      const existing = subscriptions.get(key);
      if (existing) {
        existing.active = false;
        existing.removeAbortListener();
      }
    }

    const onAbort = () =>
      this.removeSubscription(topic, subscriptions, key, subscription);

    const subscription: TopicSubscription = {
      subscriber,
      active: true,
      removeAbortListener: () => signal?.removeEventListener("abort", onAbort),
      dispatch: (args: unknown[]) => {
        const typedArgs = args as T[K];
        if (filter && !filter(...typedArgs)) return;
        if (once) {
          this.removeSubscription(topic, subscriptions, key, subscription);
        }
        subscriber(...typedArgs);
      },
    };

    signal?.addEventListener("abort", onAbort, { once: true });
    subscriptions.set(key, subscription);

    return () =>
      this.removeSubscription(topic, subscriptions, key, subscription);
  }

  private removeSubscription(
    topic: keyof T,
    subscriptions: Map<unknown, TopicSubscription>,
    key: unknown,
    subscription: TopicSubscription,
  ): void {
    if (!subscription.active) return;
    subscription.active = false;
    subscription.removeAbortListener();
    subscriptions.delete(key);
    if (
      subscriptions.size === 0 &&
      this.topicSubscriptions.get(topic) === subscriptions
    ) {
      this.topicSubscriptions.delete(topic);
    }
  }

  private clearTopicSubscriptions(
    subscriptions: Map<unknown, TopicSubscription>,
  ): void {
    for (const subscription of subscriptions.values()) {
      subscription.active = false;
      subscription.removeAbortListener();
    }
    subscriptions.clear();
  }

  private addWildcardSubscriber(
    subscriber: (message: TopicMessage<T>) => void,
  ): () => void {
    this.wildcardSubscribers.add(subscriber);
    return () => {
      this.wildcardSubscribers.delete(subscriber);
    };
  }

  private createReadableStream<U>(
    subscribe: (push: (item: U) => void) => () => void,
  ): ReadableStream<U> {
    let unsubscribe = () => {};
    let closed = false;

    return new ReadableStream<U>({
      start: (controller) => {
        unsubscribe = subscribe((item) => {
          if (!closed) {
            controller.enqueue(item);
          }
        });
      },
      cancel: () => {
        closed = true;
        unsubscribe();
      },
    });
  }

  private createAsyncGenerator<U>(
    subscribe: (push: (item: U) => void) => () => void,
    signal?: AbortSignal,
  ): AsyncGenerator<U> {
    if (signal?.aborted) {
      return (async function* () {})();
    }

    let queue: U[] = [];
    let resolveNext: (() => void) | null = null;
    let stopped = false;

    const wake = () => {
      resolveNext?.();
      resolveNext = null;
    };

    const unsubscribe = subscribe((item) => {
      if (stopped) return;
      queue.push(item);
      wake();
    });

    const stop = () => {
      if (stopped) return;
      stopped = true;
      signal?.removeEventListener("abort", stop);
      unsubscribe();
      wake();
    };

    signal?.addEventListener("abort", stop, { once: true });

    const generator = (async function* () {
      try {
        while (!stopped) {
          if (queue.length > 0) {
            const batch = queue;
            queue = [];
            for (const item of batch) {
              if (stopped) return;
              yield item;
            }
            continue;
          }

          const { promise, resolve } = Promise.withResolvers<void>();
          resolveNext = resolve;
          await promise;
        }
      } finally {
        stop();
      }
    })();

    const baseReturn = generator.return.bind(generator);
    generator.return = (value) => {
      stop();
      return baseReturn(value);
    };

    const baseThrow = generator.throw.bind(generator);
    generator.throw = (err) => {
      stop();
      return baseThrow(err);
    };

    return generator;
  }
}

export function collect<T extends TopicShape<T>, K extends keyof T>(
  pubsub: PubSub<T>,
  topic: K,
  options?: CollectOptions<T[K]>,
): Promise<T[K][]> {
  const signal = options?.signal;
  if (signal?.aborted) {
    return Promise.reject(getAbortReason(signal));
  }

  const { max, time, idle, filter } = options ?? {};

  if (
    (max !== undefined && max <= 0) ||
    (time !== undefined && time <= 0) ||
    (idle !== undefined && idle <= 0)
  ) {
    return Promise.resolve([]);
  }

  const { promise, resolve, reject } = Promise.withResolvers<T[K][]>();
  const collected: T[K][] = [];
  let settled = false;
  let durationTimer: ReturnType<typeof setTimeout> | undefined;
  let idleTimer: ReturnType<typeof setTimeout> | undefined;

  const cleanup = () => {
    clearTimeout(durationTimer);
    clearTimeout(idleTimer);
    signal?.removeEventListener("abort", onAbort);
    unsubscribe();
  };

  const complete = () => {
    if (settled) return;
    settled = true;
    cleanup();
    resolve(collected);
  };

  const onAbort = () => {
    if (settled) return;
    settled = true;
    cleanup();
    reject(getAbortReason(signal!));
  };

  const resetIdleTimer = () => {
    if (idle === undefined || !Number.isFinite(idle)) return;
    clearTimeout(idleTimer);
    idleTimer = setTimeout(complete, idle);
  };

  const unsubscribe = pubsub.sub(
    topic,
    (...args) => {
      if (settled) return;
      collected.push(args);
      if (max !== undefined && collected.length >= max) {
        complete();
        return;
      }
      resetIdleTimer();
    },
    { filter },
  );

  if (time !== undefined && Number.isFinite(time)) {
    durationTimer = setTimeout(complete, time);
  }
  resetIdleTimer();
  signal?.addEventListener("abort", onAbort, { once: true });

  return promise;
}
