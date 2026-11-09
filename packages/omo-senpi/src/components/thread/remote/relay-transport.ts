import { randomUUID } from "node:crypto"
import type { ThreadToolResult } from "../contracts"
import { threadToolFailure, type ThreadToolFailure } from "../errors"
import { hashArgs } from "../gateway/bindings"
import { decideDelivery, type TargetState } from "../gateway/decision"
import { senderFacts } from "../gateway/envelope"
import { deliveryProvenanceLine, renderDeliveryText } from "../gateway/provenance"
import { REFUSAL_CODES } from "../gateway/result"
import type { GatewayStore } from "../gateway/store"
import type { DeliveryOutcome, DeliveryTransport, OutboundDelivery } from "../gateway/transport"
import type { Provenance } from "../gateway/types"
import { UNKNOWN_CALLER } from "../tools/ports"
import { deviceOnline, outcomeFailure } from "./lazy-ops"
import type { JsonObject, RemoteCommandOutcome, RemoteSessionRow, RemoteThreadSurface } from "./ports"

export type RelayTransportOptions = {
  readonly remote: RemoteThreadSurface
  readonly store: Pick<GatewayStore, "now" | "toolReceiptBegin" | "toolReceiptSettle" | "deliveryView">
  readonly now?: () => number
  readonly senderProvenance: () => Promise<Provenance>
  readonly callerCause?: () => string | undefined
  readonly callerName?: () => string | undefined
  readonly callerChainHop?: () => number
  readonly log?: (line: string) => void
  /** Milliseconds before retry `attempt` (0-based) of a delivery held behind the receiver's draft. */
  readonly retryDelayMs?: (attempt: number) => number
}

export type RelayTransport = DeliveryTransport & { readonly dispose: () => void }

type Failure = { readonly kind: "error"; readonly error: ThreadToolFailure }
const fail = (code: ThreadToolFailure["code"], message: string, next: string, details?: Readonly<Record<string, unknown>>): Failure => ({ kind: "error", error: threadToolFailure(code, message, next, details) })

// The refusals that mean the receiving engine has no `admit_external_message` record (senpi#6233): a peer's message is
// then refused loudly, never sent as a user prompt instead.
const ADMIT_UNSUPPORTED: ReadonlySet<string> = new Set(["method_unclassified", "delegation_unclassified", "unknown_command", "engine_capability_missing", "unsupported_command", "method_unknown"])
const unsupportedReason = (reason: string): boolean => ADMIT_UNSUPPORTED.has(reason) || reason.startsWith("Unknown command")

const HELD_RETRY_MS: readonly number[] = [5_000, 15_000, 30_000, 60_000, 120_000, 300_000, 300_000, 300_000, 300_000, 300_000, 300_000]

const data = (response: JsonObject): JsonObject => {
  const value = response["data"]
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as JsonObject : {}
}

/**
 * Other-device transport. The delivery is the same one a same-device send makes - the same sender facts, the same text
 * from `renderDeliveryText`, the same decision table - and the receiving engine admits it as senpi#6233
 * `admit_external_message`: a non-user entry, deduplicated by its delivery id, that starts a turn on an idle session.
 * With no gateway row on this device, a retry under the same idempotency key replays the first answer through the tool
 * receipt store.
 */
export function createRelayTransport(options: RelayTransportOptions): RelayTransport {
  const { remote, store } = options
  const now = options.now ?? store.now
  const log = options.log ?? (() => {})
  const retryDelay = options.retryDelayMs ?? ((attempt: number) => HELD_RETRY_MS[attempt] ?? -1)
  const timers = new Set<ReturnType<typeof setTimeout>>()
  const unsettled = new Map<string, string>()
  const receiptKey = (scope: { readonly principal: string; readonly operation: string; readonly idempotency_key: string }) => `${scope.principal}\u0000${scope.operation}\u0000${scope.idempotency_key}`

  async function once(callerId: string, idempotencyKey: string, args: unknown, run: () => Promise<ThreadToolResult>): Promise<ThreadToolResult> {
    const scope = { principal: `session:${callerId}`, operation: "thread_send:remote", idempotency_key: idempotencyKey }
    const admission = await store.toolReceiptBegin({ ...scope, now: now(), args_hash: hashArgs(args) })
    if (admission.kind === "replay") { const first = admission.result as ThreadToolResult; return first.kind === "ok" && "deduplicated" in first ? { ...first, deduplicated: true } as ThreadToolResult : first }
    if (admission.kind === "conflict") return fail("idempotency_conflict", "The idempotency key was already used with different arguments.", "Retry with a new idempotency_key.")
    const note = unsettled.get(receiptKey(scope))
    if (admission.kind === "in_progress" && note !== undefined) return fail("idempotency_uncertain", "The earlier operation may have been delivered.", "Read the target transcript before deciding whether to retry.", { error_note: note })
    if (admission.kind === "in_progress") return fail("idempotency_in_progress", "The same operation is already in progress.", "Wait for the earlier call to settle, then retry.")
    if (admission.kind === "uncertain") return fail("idempotency_uncertain", "The earlier operation may have been delivered.", "Read the target transcript before deciding whether to retry.")
    const unrecorded = (what: string) => (settleError: unknown) => { unsettled.set(receiptKey(scope), `${what}, and its receipt could not be recorded: ${settleError instanceof Error ? settleError.message : String(settleError)}`) }
    let result: ThreadToolResult
    try {
      result = await run()
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      await store.toolReceiptSettle({ ...scope, now: now(), error_note: message }).catch(unrecorded(`the send failed (${message})`))
      throw error
    }
    await store.toolReceiptSettle({ ...scope, now: now(), result }).catch(unrecorded("the send ran"))
    return result
  }

  const isHeld = (outcome: RemoteCommandOutcome): boolean => outcome.kind === "ok" ? data(outcome.response)["kind"] === "held_draft" : outcome.kind === "refused" && outcome.reason === "held_draft"

  // The receiver's user is typing: the SAME record (same delivery id, so the engine dedupes it) is sent again until it is admitted.
  function retryHeld(row: RemoteSessionRow, record: JsonObject, attempt: number): void {
    const delay = retryDelay(attempt)
    if (delay < 0) { log(`relay_delivery_failed ${String(record["deliveryId"])} to ${row.session_id} on device ${row.device}: still held behind the receiver's draft`); return }
    log(`relay_delivery_held ${String(record["deliveryId"])} to ${row.session_id} on device ${row.device}: retry ${attempt + 1} in ${delay} ms`)
    const timer = setTimeout(() => {
      timers.delete(timer)
      void remote.sessions.sessionCommand(row.device, row.session_id, record).then(
        (outcome) => { if (isHeld(outcome)) retryHeld(row, record, attempt + 1) },
        (error: unknown) => log(`relay_delivery_failed ${String(record["deliveryId"])}: ${error instanceof Error ? error.message : String(error)}`),
      )
    }, delay)
    timer.unref?.()
    timers.add(timer)
  }

  async function send(delivery: OutboundDelivery, row: RemoteSessionRow): Promise<ThreadToolResult> {
    const offline = await deviceOnline(remote, row.device)
    if (offline !== undefined) return offline
    const callerId = delivery.caller_id
    const name = options.callerName?.()?.trim()
    const facts = senderFacts({ kind: "session", durable_id: callerId, ...(name === undefined || name === "" ? {} : { name }) }, await options.senderProvenance())
    const cause = options.callerCause?.()
    const prior = cause === undefined ? undefined : (await store.deliveryView(cause))?.row
    const deliveryId = randomUUID()
    const hop = Math.max(prior?.hop ?? 0, options.callerChainHop?.() ?? 0) + 1
    const rootId = prior?.root_id ?? deliveryId
    let deliverAs: "steer" | "followUp" = "followUp"
    let expected: string | undefined
    if (delivery.mode === "steer") {
      const state = await remote.sessions.sessionCommand(row.device, row.session_id, { type: "get_state", sessionId: row.session_id })
      if (state.kind !== "ok") return await outcomeFailure(state, row.device)
      const engine = data(state.response)
      const questions = engine["pendingQuestions"]
      const phase: TargetState = engine["isStreaming"] === true && engine["isCompacting"] !== true && !(Array.isArray(questions) && questions.length > 0) ? "mid_turn" : "idle"
      const current = typeof engine["currentTurnId"] === "string" && /^\d+$/.test(engine["currentTurnId"]) ? Number(engine["currentTurnId"]) : null
      // An engine that reports no running turn id (before senpi 2026.11.2) cannot have the caller's expected turn checked.
      if (phase === "mid_turn" && current === null) return fail("turn_conflict", `Turn ${delivery.expected_turn_id} of ${row.session_id} cannot be confirmed on device ${row.device}.`, REFUSAL_CODES["turn_conflict"]?.next ?? "Retry with delivery auto.", { device: row.device, reason: "remote_turn_not_verifiable" })
      const decision = decideDelivery(phase, "steer", delivery.expected_turn_id === undefined ? null : Number(delivery.expected_turn_id), current)
      if (decision.kind === "refuse") return fail(REFUSAL_CODES[decision.reason]?.code ?? decision.reason, `A steer to ${row.session_id} was refused: ${decision.reason}.`, REFUSAL_CODES[decision.reason]?.next ?? "Send with delivery auto.", { device: row.device, ...(current === null ? {} : { current_turn_id: String(current) }) })
      if (decision.kind !== "admit") return fail("turn_conflict", `The running turn of ${row.session_id} cannot be confirmed on device ${row.device}.`, REFUSAL_CODES["turn_conflict"]?.next ?? "Retry with delivery auto.", { device: row.device })
      deliverAs = "steer"
      expected = String(current)
    }
    const text = renderDeliveryText({
      delivery_id: deliveryId,
      envelope: { provenance: facts.provenance, origin: facts.origin(deliveryId), actor: facts.actor, via: [callerId], root_id: rootId, hop, delivery_id: deliveryId },
      body: delivery.text,
      mode_requested: delivery.mode,
      mode_effective: deliverAs === "steer" ? "steer" : "follow_up",
      binding_id: null,
      binding_revision: null,
    })
    const record = remote.chain.stamp({
      type: "admit_external_message",
      sessionId: row.session_id,
      deliveryId,
      text,
      displayText: `${deliveryProvenanceLine(facts.provenance)}\n${delivery.text}`,
      sender: { kind: "agent", session_id: callerId, ...(name === undefined || name === "" ? {} : { name }) },
      deliverAs,
      ...(expected === undefined ? {} : { expectedTurnId: expected }),
      chainCount: hop,
    }, hop)
    const outcome = await remote.sessions.sessionCommand(row.device, row.session_id, record)
    if (outcome.kind === "refused" && unsupportedReason(outcome.reason)) return fail("unsupported", `Device ${row.device} runs an engine without admit_external_message; update omo and senpi there.`, "Update omo and senpi on that device, then send again.", { device: row.device, reason: outcome.reason })
    const ok = (kind: "started" | "steered" | "queued", extra: Readonly<Record<string, unknown>>, deduplicated = false): ThreadToolResult => ({ kind: "ok", thread_id: row.session_id, delivery: { kind, ...extra }, deduplicated, device: row.device } as ThreadToolResult)
    if (isHeld(outcome)) { retryHeld(row, record, 0); return ok("queued", { queue_position: 1 }) }
    if (outcome.kind !== "ok") return await outcomeFailure(outcome, row.device)
    const reply = data(outcome.response)
    const turnId = typeof reply["turnId"] === "string" && reply["turnId"] !== "" ? reply["turnId"] : undefined
    switch (reply["kind"]) {
      case "turn_conflict": return fail(REFUSAL_CODES["turn_conflict"]?.code ?? "turn_conflict", `A steer to ${row.session_id} was refused: turn_conflict.`, REFUSAL_CODES["turn_conflict"]?.next ?? "Retry with delivery auto.", { device: row.device })
      case "already_admitted": return ok("queued", { queue_position: 1 }, true)
      case "steered": return ok("steered", { turn_id: turnId ?? expected ?? "" })
      case "started":
      case "queued": {
        if (reply["kind"] === "started" && turnId !== undefined) return ok("started", { turn_id: turnId })
        const after = await remote.sessions.sessionCommand(row.device, row.session_id, { type: "get_state", sessionId: row.session_id })
        const settled = after.kind === "ok" ? data(after.response) : {}
        const current = settled["currentTurnId"]
        if (reply["kind"] === "started" && typeof current === "string" && current !== "") return ok("started", { turn_id: current })
        const pending = settled["pendingMessageCount"]
        if (reply["kind"] === "queued" && typeof pending === "number" && Number.isInteger(pending) && pending >= 1) return ok("queued", { queue_position: pending })
        return fail("idempotency_uncertain", `Device ${row.device} accepted the message but did not report where it waits.`, "Read the thread before deciding whether to send again.", { device: row.device })
      }
      default: return fail("idempotency_uncertain", `Device ${row.device} accepted the message but did not report where it waits.`, "Read the thread before deciding whether to send again.", { device: row.device })
    }
  }

  return {
    deliver: async (delivery: OutboundDelivery): Promise<DeliveryOutcome> => {
      if (delivery.target.kind !== "remote") throw new Error("the relay transport delivers only to sessions on other devices")
      const row = delivery.target.row
      // Before a receipt is written, exactly where a local gateway send refuses an unknown caller.
      if (delivery.caller_id === UNKNOWN_CALLER) return fail("caller_context_missing", "A gateway send needs the calling session's durable id.", "Retry from a session that passes its execution context.")
      if (delivery.expected_turn_id !== undefined && !/^\d+$/.test(delivery.expected_turn_id)) return fail("turn_conflict", `Turn ${delivery.expected_turn_id} is not a turn of the target session.`, "Read the target again and steer with the turn_id a started or steered result returned.")
      if (delivery.mode === "steer" && delivery.expected_turn_id === undefined) return fail("invalid_arguments", "A steer needs expected_turn_id (the target's current turn_epoch).", "Fix the arguments and send again.")
      const sent = await once(delivery.caller_id, delivery.idempotency_key, delivery.args, async () => await send(delivery, row))
      if (sent.kind !== "ok" || !("delivery" in sent)) return sent.kind === "ok" ? fail("internal_error", "The remote send answered without a delivery.", "Read the target thread before retrying.") : sent
      return { kind: "ok", thread_id: row.session_id, delivery: sent.delivery, deduplicated: sent.deduplicated, device: row.device }
    },
    dispose: () => { for (const timer of timers) clearTimeout(timer); timers.clear() },
  }
}
