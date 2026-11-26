import type { Unsubscribe } from "./types.js";

export const noop = (): void => {};

export function toUnsubscribe(fn: () => void): Unsubscribe {
  (fn as Unsubscribe)[Symbol.dispose] = fn;
  return fn as Unsubscribe;
}

export const noopUnsubscribe: Unsubscribe = toUnsubscribe(noop);

export function onAbort(
  signal: AbortSignal | undefined,
  listener: () => void,
): () => void {
  if (!signal) return noop;
  signal.addEventListener("abort", listener, { once: true });
  return () => signal.removeEventListener("abort", listener);
}

export function isWildcardArgs<K, O>(
  topicOrOptions: K | O | undefined,
): topicOrOptions is O | undefined {
  return (
    topicOrOptions === undefined ||
    topicOrOptions === null ||
    typeof topicOrOptions === "object"
  );
}

export function abortError(message: string): DOMException {
  return new DOMException(message, "AbortError");
}
