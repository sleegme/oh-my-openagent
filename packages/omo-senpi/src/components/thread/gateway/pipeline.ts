import type { DeliveryOutcome, DeliveryTransport, OutboundDelivery } from "./transport"

export type DeliveryPipelineOptions = {
  readonly local: DeliveryTransport
  /** Absent when this device reaches no other device: a remote target is then refused, never sent. */
  readonly relay?: DeliveryTransport
}

export type DeliveryPipeline = {
  readonly deliver: (delivery: OutboundDelivery) => Promise<DeliveryOutcome>
}

/**
 * The one talking channel: every send (the thread tools, a handoff, the SDK) becomes one `OutboundDelivery` and goes
 * to the transport its resolved target names. Nothing past this point branches on the device again.
 */
export function createDeliveryPipeline(options: DeliveryPipelineOptions): DeliveryPipeline {
  return {
    deliver: async (delivery) => {
      if (delivery.target.kind === "local") return await options.local.deliver(delivery)
      // senpi admits a command-line delivery only from its own machine (admit-external-message-command.ts
      // `sender_not_allowed`), so a command-line sender is refused here, before any record leaves this device.
      if (delivery.sender.kind === "cli") return { kind: "error", error: { code: "unsupported", message: "A command-line sender cannot reach a session on another device: the engine admits a command-line delivery only from its own machine. Use omo remote send <device>/<session>.", next_action: "Use omo remote send <device>/<session> to prompt that session as yourself.", details: { device: delivery.target.row.device, reason: "sender_not_allowed" } } }
      if (options.relay === undefined) return { kind: "error", error: { code: "host_unavailable", message: `Thread ${delivery.target.row.session_id} is on device ${delivery.target.row.device}, which this device cannot reach.`, next_action: "Sign in to the account's device service, then retry." } }
      return await options.relay.deliver(delivery)
    },
  }
}
