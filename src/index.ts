export type TopicMap = Record<string, unknown[]>;
export type TopicShape<T> = { [K in keyof T]: unknown[] };
export type TopicMessage<T extends TopicShape<T>> = {
  [K in keyof T]: [K, ...T[K]];
}[keyof T];

export interface SubOptions<Args extends unknown[] = unknown[]> {
  signal?: AbortSignal;
  filter?: (...args: Args) => boolean;
}

export interface SubIterOptions<Args extends unknown[] = unknown[]>
  extends SubOptions<Args> {
  max?: number;
  time?: number;
  idle?: number;
}

interface AsyncStreamOptions {
  signal?: AbortSignal;
  max?: number;
  time?: number;
  idle?: number;
}

interface TopicSubscription {
  subscriber: unknown;
  active: boolean;
  cleanup: () => void;
  dispatch: (args: unknown[]) => void;
}

const NOOP = () => {};

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
    options?: SubIterOptions<T[K]>,
  ): AsyncGenerator<T[K]>;
  public sub<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
    options?: SubOptions<T[K]>,
  ): () => void;
  public sub<K extends keyof T>(
    topic: K,
    subscriberOrOptions?: ((...args: T[K]) => void) | SubIterOptions<T[K]>,
    options?: SubOptions<T[K]>,
  ): (() => void) | AsyncGenerator<T[K]> {
    if (typeof subscriberOrOptions !== "function") {
      return this.createAsyncGenerator<T[K]>(
        (push) =>
          this.addSubscription(topic, (...args) => push(args), {
            filter: subscriberOrOptions?.filter,
          }),
        subscriberOrOptions,
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
      return this.oncePromise(topic, subscriberOrOptions);
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
          this.addSubscription(topic, (...args) => push(args)),
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

  private oncePromise<K extends keyof T>(
    topic: K,
    options?: SubOptions<T[K]>,
  ): Promise<T[K]> {
    const signal = options?.signal;
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
      { filter: options?.filter, once: true },
    );

    signal?.addEventListener("abort", onAbort, { once: true });
    return promise;
  }

  private addSubscription<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
    options?: SubOptions<T[K]> & { once?: boolean },
  ): () => void {
    const signal = options?.signal;
    if (signal?.aborted) return NOOP;

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
        this.deactivateSubscription(existing);
      }
    }

    const subscription: TopicSubscription = {
      subscriber,
      active: true,
      cleanup: NOOP,
      dispatch: (args: unknown[]) => {
        const typedArgs = args as T[K];
        if (filter && !filter(...typedArgs)) return;
        if (!subscription.active) return;
        if (once) {
          this.removeSubscription(topic, subscriptions, key, subscription);
        }
        subscriber(...typedArgs);
      },
    };

    if (signal) {
      const onAbort = () =>
        this.removeSubscription(topic, subscriptions, key, subscription);
      signal.addEventListener("abort", onAbort, { once: true });
      subscription.cleanup = () => signal.removeEventListener("abort", onAbort);
    }

    subscriptions.set(key, subscription);

    return () =>
      this.removeSubscription(topic, subscriptions, key, subscription);
  }

  private deactivateSubscription(subscription: TopicSubscription): void {
    subscription.active = false;
    subscription.cleanup();
    subscription.cleanup = NOOP;
  }

  private removeSubscription(
    topic: keyof T,
    subscriptions: Map<unknown, TopicSubscription>,
    key: unknown,
    subscription: TopicSubscription,
  ): void {
    if (!subscription.active) return;
    this.deactivateSubscription(subscription);
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
      this.deactivateSubscription(subscription);
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
    let unsubscribe = NOOP;
    let closed = false;

    return new ReadableStream<U>({
      start: (controller) => {
        const unsub = subscribe((item) => {
          if (!closed) {
            controller.enqueue(item);
          }
        });
        if (closed) {
          unsub();
        } else {
          unsubscribe = unsub;
        }
      },
      cancel: () => {
        closed = true;
        unsubscribe();
        unsubscribe = NOOP;
      },
    });
  }

  private createAsyncGenerator<U>(
    subscribe: (push: (item: U) => void) => () => void,
    options?: AsyncStreamOptions,
  ): AsyncGenerator<U> {
    const { signal, max, time, idle } = options ?? {};

    if (
      signal?.aborted ||
      (max !== undefined && max <= 0) ||
      (time !== undefined && time <= 0) ||
      (idle !== undefined && idle <= 0)
    ) {
      return (async function* () {})();
    }

    let queue: U[] = [];
    let resolveNext: (() => void) | null = null;
    let count = 0;
    let ended = false;
    let aborted = false;
    let cleanedUp = false;
    let durationTimer: ReturnType<typeof setTimeout> | undefined;
    let idleTimer: ReturnType<typeof setTimeout> | undefined;
    let unsubscribe = NOOP;

    const wake = () => {
      resolveNext?.();
      resolveNext = null;
    };

    const cleanup = () => {
      if (cleanedUp) return;
      cleanedUp = true;
      clearTimeout(durationTimer);
      clearTimeout(idleTimer);
      signal?.removeEventListener("abort", abort);
      unsubscribe();
      unsubscribe = NOOP;
    };

    const end = () => {
      if (ended) return;
      ended = true;
      cleanup();
      wake();
    };

    const abort = () => {
      if (aborted) return;
      aborted = true;
      ended = true;
      queue = [];
      cleanup();
      wake();
    };

    const resetIdleTimer = () => {
      if (idle === undefined || !Number.isFinite(idle)) return;
      clearTimeout(idleTimer);
      idleTimer = setTimeout(end, idle);
    };

    const unsub = subscribe((item) => {
      if (ended) return;
      queue.push(item);
      count++;
      if (max !== undefined && count >= max) {
        end();
        return;
      }
      resetIdleTimer();
      wake();
    });

    if (ended) {
      unsub();
    } else {
      unsubscribe = unsub;
      if (time !== undefined && Number.isFinite(time)) {
        durationTimer = setTimeout(end, time);
      }
      resetIdleTimer();
      signal?.addEventListener("abort", abort, { once: true });
    }

    const generator = (async function* () {
      try {
        while (!aborted) {
          if (queue.length > 0) {
            const batch = queue;
            queue = [];
            for (const item of batch) {
              if (aborted) return;
              yield item;
            }
            continue;
          }

          if (ended) return;

          const { promise, resolve } = Promise.withResolvers<void>();
          resolveNext = resolve;
          await promise;
        }
      } finally {
        abort();
      }
    })();

    // Ensure cleanup runs even if return/throw is called before the first next()
    const baseReturn = generator.return.bind(generator);
    generator.return = (value) => {
      abort();
      return baseReturn(value);
    };

    const baseThrow = generator.throw.bind(generator);
    generator.throw = (err) => {
      abort();
      return baseThrow(err);
    };

    return generator;
  }
}
