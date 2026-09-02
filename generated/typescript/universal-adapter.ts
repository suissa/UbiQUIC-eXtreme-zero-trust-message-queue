// GENERATED from schemas/universal-adapter.schema.yml.
export type Health = "healthy" | "degraded" | "unhealthy";

export interface Envelope {
  canonicalEvent: string;
  messageId: string;
  interactionId: string;
  correlationId: string;
  payload: Uint8Array;
}

export interface Delivery {
  deliveryId: string;
  canonicalEvent: string;
  transportName: string;
}

export interface Capabilities {
  reliable: boolean;
  ordered: boolean;
  durable: boolean;
  replay: boolean;
  requestReply: boolean;
  pubSub: boolean;
}

export interface UniversalAdapter {
  open(configYaml: string): Promise<void>;
  close(): Promise<void>;
  publish(envelope: Envelope): Promise<void>;
  subscribe(canonicalEvent: string, handler: (envelope: Envelope, delivery: Delivery) => Promise<void>): Promise<() => Promise<void>>;
  ack(delivery: Delivery): Promise<void>;
  nack(delivery: Delivery, reason: string): Promise<void>;
  health(): Promise<Health>;
  capabilities(): Capabilities;
  mapCanonicalToTransport(canonicalEvent: string): string;
  mapTransportToCanonical(transportName: string): string;
}

export type AdapterFactory = () => UniversalAdapter;
export type AdapterRegistry = ReadonlyMap<string, AdapterFactory>;

export function assertCanonicalRoundTrip(adapter: UniversalAdapter, canonicalEvent: string): void {
  const transport = adapter.mapCanonicalToTransport(canonicalEvent);
  const restored = adapter.mapTransportToCanonical(transport);
  if (restored !== canonicalEvent) {
    throw new Error(`non-reversible event mapping: ${canonicalEvent} -> ${transport} -> ${restored}`);
  }
}
