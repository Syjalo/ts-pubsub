export type TopicMap = Record<string, unknown[]>;

export type TopicMessage<T extends Record<keyof T, unknown[]>> = {
  [K in keyof T]: [K, ...T[K]];
}[keyof T];

export type Unsubscribe = (() => void) & Disposable;

export interface LimitOptions {
  signal?: AbortSignal;
  max?: number;
  duration?: number;
  idle?: number;
}

export interface SubscribeOptions<Args extends unknown[] = unknown[]> {
  signal?: AbortSignal;
  filter?: (...args: Args) => boolean;
}

export interface StreamOptions<Args extends unknown[] = unknown[]>
  extends SubscribeOptions<Args>, LimitOptions {}

export interface WildcardStreamOptions<
  T extends Record<keyof T, unknown[]> = TopicMap,
> extends LimitOptions {
  filter?: (message: TopicMessage<T>) => boolean;
}
