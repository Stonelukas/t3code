import {
  CommandId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ThreadProjection,
  type OrchestratorMcpProviderMaintenanceInput,
  type OrchestratorMcpProviderMaintenanceResult,
  type ProviderSessionId,
  type ProviderInstanceId,
  type RunId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { EventSinkV2 } from "./EventSink.ts";
import { ProjectionStoreV2 } from "./ProjectionStore.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { RuntimePolicyV2 } from "./RuntimePolicy.ts";
import { ThreadCommandExecutor } from "./ThreadCommandExecutor.ts";
import * as MaintenanceAdmission from "./ProviderMaintenanceAdmission.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { EffectOutboxV2 } from "./EffectOutbox.ts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";

export class ProviderMaintenanceError extends Schema.TaggedError<ProviderMaintenanceError>()(
  "ProviderMaintenanceError",
  { threadId: ThreadId, reason: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {
  override get message(): string {
    return this.reason;
  }
}
export const isProviderMaintenanceError = Schema.is(ProviderMaintenanceError);

/** Reconstruct the fence before startup admits effects or delegated deliveries; never replay maintenance. */
export const restoreProviderMaintenanceAdmission = Effect.gen(function* () {
  const projections = yield* ProjectionStoreV2;
  const admission = yield* MaintenanceAdmission.ProviderMaintenanceAdmission;
  const threadIds = yield* projections.getBlockingMaintenanceThreadIds;
  if (threadIds.length > 128)
    return yield* new ProviderMaintenanceError({
      threadId: ThreadId.make("maintenance:recovery"),
      reason: "Maintenance recovery exceeded its bounded reservation set.",
    });
  const held = [];
  for (const threadId of threadIds) {
    const thread = yield* projections.getThread(threadId);
    if (providerMaintenanceBlocksWork(thread) && thread.providerMaintenance !== undefined)
      held.push({ threadId, ...thread.providerMaintenance });
  }
  for (const reservation of held.sort(
    (a, b) => Number(b.scope === "environment") - Number(a.scope === "environment"),
  )) {
    const scope: MaintenanceAdmission.MaintenanceRequest = {
      scope: reservation.scope,
      threadId: reservation.threadId,
      requestId: reservation.requestId,
    };
    if ((yield* admission.snapshot(scope)).reservations.length === 0)
      yield* admission.reserve(scope);
  }
});

/** Unknown shutdown stays reserved too: a subsequent turn must not reopen it implicitly. */
export function providerMaintenanceBlocksWork(
  thread: Pick<OrchestrationV2AppThread, "providerMaintenance">,
) {
  return ["pending", "running", "unknown"].includes(thread.providerMaintenance?.status ?? "");
}

/** Admission uses all recorded work, not just the latest run or completion-alert exemptions. */
export function providerMaintenanceRefusal(
  projection: Pick<
    OrchestrationV2ThreadProjection,
    | "thread"
    | "runs"
    | "nodes"
    | "runtimeRequests"
    | "providerTurns"
    | "providerThreads"
    | "subagents"
    | "turnItems"
  >,
  options: { readonly requestingRunId?: RunId; readonly validateTarget?: boolean } = {},
): string | undefined {
  const active = (status: string) =>
    ["preparing", "queued", "starting", "pending", "running", "waiting"].includes(status);
  const exemptNodeIds = new Set(
    projection.nodes
      .filter(
        (node) => node.runId === options.requestingRunId && options.requestingRunId !== undefined,
      )
      .map((node) => node.id),
  );
  const requestingRun = projection.runs.find(
    (run) => run.id === options.requestingRunId && run.status === "running",
  );
  if (
    options.validateTarget !== false &&
    (projection.thread.archivedAt !== null || projection.thread.deletedAt !== null)
  )
    return "Reopen this thread before restarting its Pi RPC session.";
  if (
    projection.runs.some((run) => active(run.status) && run !== requestingRun) ||
    projection.nodes.some((node) => active(node.status) && !exemptNodeIds.has(node.id)) ||
    projection.providerTurns.some((turn) => active(turn.status) && !exemptNodeIds.has(turn.nodeId))
  )
    return "Provider maintenance requires no other active or queued work.";
  if (projection.runtimeRequests.some((request) => request.status === "pending"))
    return "Answer or dismiss the pending requests before restarting Pi RPC.";
  if (
    projection.providerThreads.some(
      (thread) =>
        (thread.status === "active" && thread.id !== requestingRun?.providerThreadId) ||
        (thread.pendingBackgroundTasks?.length ?? 0) > 0,
    ) ||
    projection.subagents.some(
      (task) =>
        active(task.status) ||
        ["pending", "claimed"].includes(task.completionDelivery?.state ?? ""),
    ) ||
    projection.runs.some((run) => run.delegatedCompletion?.disposition === "open") ||
    projection.turnItems.some(
      (item) =>
        ["command_execution", "dynamic_tool", "subagent"].includes(item.type) &&
        active(item.status) &&
        (requestingRun === undefined || item.runId !== requestingRun.id),
    ) ||
    (projection.thread.pullRequests ?? []).some((link) => link.watch !== undefined)
  )
    return "Wait for or stop outstanding background and delegated work before restarting Pi RPC.";
  return undefined;
}

export interface ProviderMaintenanceServiceShape {
  readonly execute: (input: {
    readonly threadId: ThreadId;
    readonly requestId: CommandId;
  }) => Effect.Effect<void, ProviderMaintenanceError>;
  readonly sharedReload: (input: {
    readonly caller: {
      readonly threadId: ThreadId;
      readonly providerSessionId: ProviderSessionId;
      readonly providerInstanceId: ProviderInstanceId;
      readonly namespace: string;
      readonly issuedAt: number;
    };
    readonly requestId: CommandId;
    readonly phase: OrchestratorMcpProviderMaintenanceInput["phase"];
    readonly outcome?: "succeeded" | "failed";
  }) => Effect.Effect<OrchestratorMcpProviderMaintenanceResult, ProviderMaintenanceError>;
  readonly restoreAdmission: Effect.Effect<void, ProviderMaintenanceError>;
  readonly armDeadline: (input: {
    readonly threadId: ThreadId;
    readonly requestId: CommandId;
  }) => Effect.Effect<void, ProviderMaintenanceError>;
}

export class ProviderMaintenanceService extends Context.Service<
  ProviderMaintenanceService,
  ProviderMaintenanceServiceShape
>()("t3/orchestration-v2/ProviderMaintenanceService") {}

export const layer = Layer.effect(
  ProviderMaintenanceService,
  Effect.gen(function* () {
    const projections = yield* ProjectionStoreV2;
    const sessions = yield* ProviderSessionManagerV2;
    const policy = yield* RuntimePolicyV2;
    const commands = yield* ThreadCommandExecutor;
    const events = yield* EventSinkV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const admission = yield* MaintenanceAdmission.ProviderMaintenanceAdmission;
    const outbox = yield* EffectOutboxV2;
    const serviceScope = yield* Effect.scope;
    const reloadBarriers = new Map<
      CommandId,
      ReadonlyArray<ProviderAdapter.ProviderAdapterV2SessionRuntime>
    >();
    const operationLock = yield* KeyedLock.make<CommandId>();

    const record = (
      input: { readonly threadId: ThreadId; readonly requestId: CommandId },
      status: NonNullable<OrchestrationV2AppThread["providerMaintenance"]>["status"],
      message: string | null,
    ) =>
      commands.withLock(
        input.threadId,
        Effect.gen(function* () {
          const thread = yield* projections.getThread(input.threadId);
          if (thread.providerMaintenance?.requestId !== input.requestId)
            return yield* new ProviderMaintenanceError({
              threadId: input.threadId,
              reason: "The Pi RPC restart reservation changed.",
            });
          const now = yield* DateTime.now;
          const commandId = CommandId.make(
            `command:pi-session-restart:${status}:${input.requestId}`,
          );
          yield* events.write({
            commandId,
            events: [
              {
                id: yield* ids.allocate.event({ threadId: input.threadId, commandId }),
                type: "thread.metadata-updated",
                threadId: input.threadId,
                providerInstanceId: thread.providerInstanceId,
                occurredAt: now,
                payload: {
                  ...thread,
                  providerMaintenance: { ...thread.providerMaintenance, status, message },
                  updatedAt: now,
                },
              },
            ],
          });
        }),
      );

    const lease = (
      input: { readonly threadId: ThreadId; readonly requestId: CommandId },
      gate: MaintenanceAdmission.Reservation,
      deadlineEpochMs: number,
    ) =>
      Effect.gen(function* () {
        const delay = Math.max(0, deadlineEpochMs - DateTime.toEpochMillis(yield* DateTime.now));
        yield* Effect.sleep(delay);
        yield* operationLock.withLock(
          input.requestId,
          Effect.gen(function* () {
            const thread = yield* projections.getThread(input.threadId);
            const held = thread.providerMaintenance;
            if (
              held?.requestId !== input.requestId ||
              held.deadlineEpochMs !== deadlineEpochMs ||
              !["pending", "running"].includes(held.status)
            )
              return;
            if (held.status === "pending") {
              yield* record(
                input,
                "expired",
                "Provider maintenance expired before execution. Nothing was reloaded; request a new operation explicitly.",
              );
              yield* admission.release(gate);
            } else {
              yield* record(
                input,
                "unknown",
                "Provider maintenance did not acknowledge completion before its deadline. It was not retried. New work remains blocked until containment is verified.",
              );
            }
          }),
        );
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("Failed to expire provider maintenance", { ...input, cause }),
        ),
        Effect.forkIn(serviceScope),
        Effect.asVoid,
      );

    const globalInspection = (
      input: { readonly threadId: ThreadId; readonly requestId: CommandId },
      requestingRunId?: RunId,
    ) =>
      Effect.gen(function* () {
        const candidates = yield* projections.getMaintenanceThreadIds;
        if (candidates.length > 128)
          return yield* new ProviderMaintenanceError({
            threadId: input.threadId,
            reason:
              "Too many outstanding runtime records to verify maintenance within its bound. Stop unused sessions and try again.",
          });
        const runtimes = new Map<
          ProviderSessionId,
          ProviderAdapter.ProviderAdapterV2SessionRuntime
        >();
        for (const threadId of new Set([input.threadId, ...candidates])) {
          const projection = yield* projections.getThreadRecords(threadId, [
            "runs",
            "nodes",
            "runtimeRequests",
            "providerSessions",
            "providerThreads",
            "providerTurns",
            "subagents",
            "turnItems",
          ]);
          const refusal = providerMaintenanceRefusal(projection, {
            validateTarget: false,
            ...(threadId === input.threadId && requestingRunId !== undefined
              ? { requestingRunId }
              : {}),
          });
          if (refusal !== undefined)
            return yield* new ProviderMaintenanceError({
              threadId: input.threadId,
              reason: `${threadId}: ${refusal}`,
            });
          if (
            providerMaintenanceBlocksWork(projection.thread) &&
            projection.thread.providerMaintenance?.requestId !== input.requestId
          )
            return yield* new ProviderMaintenanceError({
              threadId: input.threadId,
              reason: "Another provider maintenance operation is reserved or unconfirmed.",
            });
          if (yield* outbox.hasUnsettled(threadId))
            return yield* new ProviderMaintenanceError({
              threadId: input.threadId,
              reason: "Wait for outstanding lifecycle effects before provider maintenance.",
            });
          for (const session of projection.providerSessions) {
            const found = yield* sessions.get(session.id);
            if (Option.isNone(found)) continue;
            const runtime = found.value;
            const isRequestingRuntime =
              threadId === input.threadId &&
              projection.runs.some(
                (run) =>
                  run.id === requestingRunId &&
                  run.status === "running" &&
                  projection.providerThreads.some(
                    (providerThread) =>
                      providerThread.id === run.providerThreadId &&
                      providerThread.providerSessionId === session.id,
                  ),
              );
            if (
              (runtime.providerSession.status !== "ready" && !isRequestingRuntime) ||
              (yield* runtime.hasPendingBackgroundWork ?? Effect.succeed(false))
            )
              return yield* new ProviderMaintenanceError({
                threadId: input.threadId,
                reason: "A provider runtime is busy or has a pending native wake.",
              });
            runtimes.set(session.id, runtime);
          }
        }
        return [...runtimes.values()];
      });

    return ProviderMaintenanceService.of({
      armDeadline: (input) =>
        Effect.gen(function* () {
          const thread = yield* projections.getThread(input.threadId);
          const reservation = thread.providerMaintenance;
          if (reservation?.requestId !== input.requestId) return;
          const scope: MaintenanceAdmission.AdmissionScope = {
            scope: reservation.scope,
            threadId: input.threadId,
          };
          const gate = (yield* admission.snapshot(scope)).reservations.find(
            (candidate) => candidate.requestId === input.requestId,
          );
          if (gate !== undefined) yield* lease(input, gate, reservation.deadlineEpochMs);
        }).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderMaintenanceError({
                threadId: input.threadId,
                reason: "Could not arm the maintenance deadline.",
                cause,
              }),
          ),
        ),
      restoreAdmission: restoreProviderMaintenanceAdmission.pipe(
        Effect.provideService(ProjectionStoreV2, projections),
        Effect.provideService(MaintenanceAdmission.ProviderMaintenanceAdmission, admission),
        Effect.mapError((cause) =>
          isProviderMaintenanceError(cause)
            ? cause
            : new ProviderMaintenanceError({
                threadId: ThreadId.make("maintenance:recovery"),
                reason: "Could not restore the provider maintenance fence.",
                cause,
              }),
        ),
      ),
      sharedReload: (input) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const threadId = input.caller.threadId;
            const thread = yield* projections.getThread(threadId);
            if (
              thread.runtimeMode !== "full-access" ||
              thread.interactionMode !== "default" ||
              thread.archivedAt !== null ||
              thread.deletedAt !== null ||
              thread.providerInstanceId !== input.caller.providerInstanceId
            )
              return yield* new ProviderMaintenanceError({
                threadId,
                reason:
                  "Provider maintenance requires the current open full-access/default Pi caller.",
              });
            let held = thread.providerMaintenance;
            const context = { threadId, requestId: input.requestId };
            const scope = { ...context, scope: "environment" as const };
            if (
              held?.requestId === input.requestId &&
              (held.callerNamespace !== input.caller.namespace ||
                held.callerIssuedAt !== input.caller.issuedAt ||
                held.providerSessionId !== input.caller.providerSessionId ||
                held.kind !== "suite-reload")
            )
              return yield* new ProviderMaintenanceError({
                threadId,
                reason:
                  "Provider maintenance belongs to a different caller or runtime incarnation.",
              });
            if (input.phase === "reserve" && held?.requestId !== input.requestId) {
              if (
                providerMaintenanceBlocksWork(thread) ||
                thread.archivedAt !== null ||
                thread.deletedAt !== null
              )
                return yield* new ProviderMaintenanceError({
                  threadId,
                  reason: "This thread already has a reservation or is not open.",
                });
              const projection = yield* projections.getThreadRecords(threadId, [
                "providerThreads",
                "runs",
              ]);
              const providerThread = projection.providerThreads.find(
                (candidate) => candidate.id === thread.activeProviderThreadId,
              );
              if (
                providerThread?.driver !== "pi" ||
                providerThread.providerSessionId !== input.caller.providerSessionId ||
                providerThread.providerInstanceId !== input.caller.providerInstanceId ||
                providerThread.nativeThreadRef?.nativeId == null
              )
                return yield* new ProviderMaintenanceError({
                  threadId,
                  reason: "Shared Suite reload requires the caller's saved Pi runtime.",
                });
              const requestingRun = projection.runs.find(
                (run) => run.status === "running" && run.providerThreadId === providerThread.id,
              );
              yield* admission.withMaintenanceAdmission(
                scope,
                Effect.gen(function* () {
                  let transferred = false;
                  yield* Effect.acquireUseRelease(
                    admission.reserve(scope),
                    (gate) =>
                      Effect.gen(function* () {
                        yield* restore(globalInspection(context, requestingRun?.id));
                        const now = yield* DateTime.now;
                        const reservation = {
                          requestId: input.requestId,
                          kind: "suite-reload" as const,
                          scope: "environment" as const,
                          providerSessionId: input.caller.providerSessionId,
                          providerThreadId: providerThread.id,
                          callerNamespace: input.caller.namespace,
                          callerIssuedAt: input.caller.issuedAt,
                          requestingRunId: requestingRun?.id ?? null,
                          deadlineEpochMs: DateTime.toEpochMillis(now) + 120_000,
                          status: "pending" as const,
                          message:
                            "Reserved only. Wait for the requesting foreground turn to drain, then call begin. This is not permission to reload.",
                        };
                        yield* commands.withLock(
                          threadId,
                          Effect.gen(function* () {
                            const current = yield* projections.getThread(threadId);
                            if (
                              current.runtimeMode !== "full-access" ||
                              current.interactionMode !== "default" ||
                              current.archivedAt !== null ||
                              current.deletedAt !== null ||
                              current.providerInstanceId !== input.caller.providerInstanceId ||
                              current.activeProviderThreadId !== providerThread.id
                            )
                              return yield* new ProviderMaintenanceError({
                                threadId,
                                reason:
                                  "The caller changed before maintenance acquired its durable reservation.",
                              });
                            yield* events.write({
                              commandId: input.requestId,
                              events: [
                                {
                                  id: yield* ids.allocate.event({
                                    threadId,
                                    commandId: input.requestId,
                                  }),
                                  type: "thread.metadata-updated",
                                  threadId,
                                  providerInstanceId: current.providerInstanceId,
                                  occurredAt: now,
                                  payload: {
                                    ...current,
                                    providerMaintenance: reservation,
                                    updatedAt: now,
                                  },
                                },
                              ],
                            });
                          }),
                        );
                        yield* lease(context, gate, reservation.deadlineEpochMs);
                        transferred = true;
                      }),
                    (gate) =>
                      transferred ? Effect.void : admission.release(gate).pipe(Effect.asVoid),
                  );
                }),
              );
              held = (yield* projections.getThread(threadId)).providerMaintenance;
            }
            if (held?.requestId !== input.requestId)
              return yield* new ProviderMaintenanceError({
                threadId,
                reason:
                  "The exact provider maintenance reservation was not found. No operation was queued.",
              });
            const snapshot = yield* admission.snapshot(scope);
            const gate = snapshot.reservations.find(
              (candidate) => candidate.requestId === input.requestId,
            );
            if (
              held.status === "pending" &&
              DateTime.toEpochMillis(yield* DateTime.now) >= held.deadlineEpochMs
            ) {
              yield* record(
                context,
                "expired",
                "Provider maintenance expired before execution. Request a new operation explicitly.",
              );
              if (gate !== undefined) yield* admission.release(gate);
              held = (yield* projections.getThread(threadId)).providerMaintenance!;
            }
            if (
              held.status === "running" &&
              DateTime.toEpochMillis(yield* DateTime.now) >= held.deadlineEpochMs
            ) {
              yield* record(
                context,
                "unknown",
                "The reload lease expired after begin. No retry or new work is authorized until containment is verified.",
              );
              held = (yield* projections.getThread(threadId)).providerMaintenance!;
            }
            if (input.phase === "cancel" && held.status === "pending") {
              yield* record(
                context,
                "cancelled",
                "Provider maintenance cancelled before execution. Nothing was reloaded.",
              );
              if (gate !== undefined) yield* admission.release(gate);
            } else if (input.phase === "begin" && held.status === "pending") {
              if (gate === undefined)
                return yield* new ProviderMaintenanceError({
                  threadId,
                  reason: "The process-local maintenance fence is unavailable. Begin was refused.",
                });
              const entered: Array<ProviderAdapter.ProviderAdapterV2SessionRuntime> = [];
              const start = yield* Effect.exit(
                restore(
                  Effect.gen(function* () {
                    const runtimes = yield* globalInspection(context);
                    for (const runtime of runtimes) {
                      if (
                        runtime.driver !== "pi" ||
                        runtime.enterMaintenance === undefined ||
                        runtime.recheckMaintenance === undefined ||
                        runtime.exitMaintenance === undefined
                      )
                        return yield* new ProviderMaintenanceError({
                          threadId,
                          reason:
                            "Close runtimes without Pi maintenance barriers before shared Suite reload.",
                        });
                      // Track before entering: a native race may leave an invalidated barrier.
                      entered.push(runtime);
                      yield* runtime.enterMaintenance({ requestId: input.requestId });
                    }
                    if (
                      !entered.some(
                        (runtime) => runtime.providerSessionId === input.caller.providerSessionId,
                      )
                    )
                      return yield* new ProviderMaintenanceError({
                        threadId,
                        reason: "The requesting Pi runtime is no longer attached.",
                      });
                    for (const runtime of entered)
                      yield* runtime.recheckMaintenance!({ requestId: input.requestId });
                    if (DateTime.toEpochMillis(yield* DateTime.now) >= held.deadlineEpochMs)
                      return yield* new ProviderMaintenanceError({
                        threadId,
                        reason:
                          "Maintenance expired during its native checks. No reload was granted.",
                      });
                    yield* admission.begin(gate, snapshot.generation);
                    yield* record(
                      context,
                      "running",
                      "One reload attempt admitted. No model continuation or retry is authorized by this receipt.",
                    );
                    reloadBarriers.set(input.requestId, entered);
                  }).pipe(Effect.timeout("20 seconds")),
                ),
              );
              if (Exit.isFailure(start)) {
                const cleanup = yield* Effect.exit(
                  restore(
                    Effect.forEach(
                      entered,
                      (runtime) =>
                        Effect.exit(
                          runtime.exitMaintenance!({ requestId: input.requestId }).pipe(
                            Effect.timeout("5 seconds"),
                          ),
                        ),
                      { concurrency: "unbounded" },
                    ),
                  ),
                );
                const uncertain =
                  Exit.isFailure(cleanup) ||
                  cleanup.value.some((result) => result._tag === "Failure");
                yield* record(
                  context,
                  uncertain ? "unknown" : "failed",
                  uncertain
                    ? "Native maintenance could not be safely released. No reload grant was returned; new work remains blocked."
                    : "Provider maintenance was refused at execution. No reload was granted; request a new operation explicitly.",
                );
                if (!uncertain) yield* admission.release(gate);
                return yield* Effect.failCause(start.cause);
              }
              return {
                reservation: (yield* projections.getThread(threadId)).providerMaintenance!,
                mayExecute: true,
              };
            } else if (input.phase === "finish") {
              if (!["succeeded", "failed"].includes(held.status)) {
                if (held.status !== "running" || gate === undefined || input.outcome === undefined)
                  return yield* new ProviderMaintenanceError({
                    threadId,
                    reason:
                      "Finish needs the exact running, unexpired attempt and an acknowledged outcome. Unknown attempts cannot be silently released.",
                  });
                const barriers = reloadBarriers.get(input.requestId);
                if (barriers === undefined)
                  return yield* new ProviderMaintenanceError({
                    threadId,
                    reason: "The native maintenance owner was lost. Finish was refused.",
                  });
                yield* Effect.gen(function* () {
                  const budget = Math.min(
                    20_000,
                    Math.max(0, held.deadlineEpochMs - DateTime.toEpochMillis(yield* DateTime.now)),
                  );
                  yield* restore(
                    Effect.gen(function* () {
                      for (const runtime of barriers)
                        yield* runtime.recheckMaintenance!({ requestId: input.requestId });
                      for (const runtime of barriers)
                        yield* runtime.exitMaintenance!({ requestId: input.requestId });
                    }).pipe(Effect.timeout(budget)),
                  );
                  if (DateTime.toEpochMillis(yield* DateTime.now) >= held.deadlineEpochMs)
                    return yield* new ProviderMaintenanceError({
                      threadId,
                      reason:
                        "The reload deadline passed before acknowledgment. Containment must be verified before releasing new work.",
                    });
                  yield* record(
                    context,
                    input.outcome!,
                    input.outcome === "succeeded"
                      ? "Suite reload acknowledged. Any continuation is selected separately by the caller."
                      : "Suite reload failed and its caller acknowledged completion. It was not retried.",
                  );
                  yield* admission.release(gate);
                  reloadBarriers.delete(input.requestId);
                }).pipe(
                  Effect.onError(() =>
                    record(
                      context,
                      "unknown",
                      "The reload outcome or native release could not be verified. New work remains blocked; no retry was scheduled.",
                    ).pipe(
                      Effect.catchCause((cause) =>
                        Effect.logError("Could not record unknown maintenance outcome", {
                          ...context,
                          cause,
                        }),
                      ),
                    ),
                  ),
                );
              }
            } else if (input.phase === "cancel" && held.status === "running") {
              return yield* new ProviderMaintenanceError({
                threadId,
                reason:
                  "An admitted reload cannot be cancelled as though it never began. Acknowledge its outcome before the deadline.",
              });
            } else if (
              (input.phase === "begin" || input.phase === "cancel") &&
              held.status === "unknown"
            ) {
              return yield* new ProviderMaintenanceError({
                threadId,
                reason:
                  "The previous attempt is unconfirmed. Verify containment before recovery; no new reload is authorized.",
              });
            }
            return {
              reservation: (yield* projections.getThread(threadId)).providerMaintenance!,
              mayExecute: false,
            };
          }),
        ).pipe(
          (effect) => operationLock.withLock(input.requestId, effect),
          Effect.mapError((cause) =>
            isProviderMaintenanceError(cause)
              ? cause
              : new ProviderMaintenanceError({
                  threadId: input.caller.threadId,
                  reason:
                    "Provider maintenance could not complete its requested transition. Do not retry an unknown begin.",
                  cause,
                }),
          ),
        ),
      execute: (input) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const projection = yield* projections.getThreadRecords(input.threadId, [
              "runs",
              "nodes",
              "runtimeRequests",
              "providerSessions",
              "providerThreads",
              "providerTurns",
              "subagents",
              "turnItems",
            ]);
            const reservation = projection.thread.providerMaintenance;
            if (reservation?.requestId !== input.requestId || reservation.kind !== "pi-rpc-restart")
              return yield* new ProviderMaintenanceError({
                threadId: input.threadId,
                reason: "This Pi RPC restart was superseded. It will not be replayed.",
              });
            if (reservation.status === "succeeded") return;
            if (
              reservation.status === "pending" &&
              DateTime.toEpochMillis(yield* DateTime.now) >= reservation.deadlineEpochMs
            ) {
              yield* record(
                input,
                "expired",
                "Pi RPC restart expired before it began. No process was restarted. Request a new restart explicitly.",
              );
              const snapshot = yield* admission.snapshot({
                scope: "thread",
                threadId: input.threadId,
              });
              const expiredGate = snapshot.reservations.find(
                (held) => held.requestId === input.requestId,
              );
              if (expiredGate !== undefined) yield* admission.release(expiredGate);
              return yield* new ProviderMaintenanceError({
                threadId: input.threadId,
                reason: "This Pi RPC restart expired before execution.",
              });
            }
            if (reservation.status !== "pending") {
              if (reservation.status === "running")
                yield* record(
                  input,
                  "unknown",
                  "The previous Pi RPC restart attempt did not confirm its outcome. It will not be replayed.",
                );
              return yield* new ProviderMaintenanceError({
                threadId: input.threadId,
                reason: "This Pi RPC restart has already been attempted. It will not be replayed.",
              });
            }
            let shutdownStarted = false;
            let oldShutdownConfirmed = false;
            let replacementOpened = false;
            let replacementOpenAttempted = false;
            let barrierAttempted = false;
            let oldRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime | undefined;
            const inspection = yield* admission.snapshot({
              scope: "thread",
              threadId: input.threadId,
            });
            const gate = inspection.reservations.find((held) => held.requestId === input.requestId);
            if (gate === undefined) {
              yield* record(
                input,
                "unknown",
                "The process-local maintenance owner is unavailable. The operation was not replayed.",
              );
              return yield* new ProviderMaintenanceError({
                threadId: input.threadId,
                reason: "The maintenance reservation has no execution owner.",
              });
            }
            const attempt = yield* Effect.exit(
              restore(
                Effect.gen(function* () {
                  const refusal = providerMaintenanceRefusal(projection);
                  if (refusal !== undefined)
                    return yield* new ProviderMaintenanceError({
                      threadId: input.threadId,
                      reason: refusal,
                    });
                  const providerThread = projection.providerThreads.find(
                    (thread) => thread.id === reservation.providerThreadId,
                  );
                  const previousSession = projection.providerSessions.find(
                    (session) => session.id === reservation.providerSessionId,
                  );
                  if (
                    providerThread?.driver !== "pi" ||
                    previousSession?.driver !== "pi" ||
                    providerThread.nativeThreadRef?.nativeId === undefined
                  )
                    return yield* new ProviderMaintenanceError({
                      threadId: input.threadId,
                      reason:
                        "The saved Pi conversation is unavailable. No replacement conversation was created.",
                    });
                  const existing = yield* sessions.get(reservation.providerSessionId);
                  if (
                    Option.isNone(existing) ||
                    existing.value.enterMaintenance === undefined ||
                    existing.value.recheckMaintenance === undefined ||
                    existing.value.exitMaintenance === undefined
                  )
                    return yield* new ProviderMaintenanceError({
                      threadId: input.threadId,
                      reason: "The saved Pi runtime is not available for a verified restart.",
                    });
                  oldRuntime = existing.value;
                  if (
                    existing.value.providerSession.status !== "ready" ||
                    (yield* existing.value.hasPendingBackgroundWork ?? Effect.succeed(false))
                  )
                    return yield* new ProviderMaintenanceError({
                      threadId: input.threadId,
                      reason:
                        "Pi is still active or has a pending native wake. Restart was refused.",
                    });
                  barrierAttempted = true;
                  const selection = yield* existing.value.enterMaintenance({
                    requestId: input.requestId,
                  });
                  const runtimePolicy = yield* policy.resolve({
                    thread: projection.thread,
                    modelSelection: selection,
                  });
                  yield* existing.value.recheckMaintenance({ requestId: input.requestId });
                  if (DateTime.toEpochMillis(yield* DateTime.now) >= reservation.deadlineEpochMs)
                    return yield* new ProviderMaintenanceError({
                      threadId: input.threadId,
                      reason: "Pi restart expired during native checks. No shutdown was admitted.",
                    });
                  yield* admission.begin(gate, inspection.generation);
                  yield* record(input, "running", null);
                  yield* admission.withOwnerAdmission(
                    gate,
                    Effect.gen(function* () {
                      shutdownStarted = true;
                      yield* sessions.release({
                        providerSessionId: reservation.providerSessionId,
                        reason: "manual_shutdown",
                        requireConfirmedShutdown: true,
                      });
                      oldShutdownConfirmed = true;
                      replacementOpenAttempted = true;
                      const replacement = yield* sessions.open({
                        threadId: input.threadId,
                        providerSessionId: reservation.providerSessionId,
                        modelSelection: selection,
                        runtimePolicy,
                        resumeFromSession: previousSession,
                        maintenanceRequestId: input.requestId,
                      });
                      replacementOpened = true;
                      const resumed = yield* replacement.resumeThread({
                        threadId: input.threadId,
                        providerThread,
                        modelSelection: selection,
                        runtimePolicy,
                      });
                      if (
                        resumed.id !== providerThread.id ||
                        resumed.nativeThreadRef?.nativeId !==
                          providerThread.nativeThreadRef?.nativeId ||
                        replacement.recheckMaintenance === undefined ||
                        replacement.exitMaintenance === undefined ||
                        (yield* replacement.hasPendingBackgroundWork ?? Effect.succeed(false))
                      )
                        return yield* new ProviderMaintenanceError({
                          threadId: input.threadId,
                          reason:
                            "Pi could not verify the original saved conversation. No prompt was sent.",
                        });
                      const restored = yield* replacement.recheckMaintenance({
                        requestId: input.requestId,
                      });
                      if (
                        restored.model !== selection.model ||
                        restored.options?.find((option) => option.id === "thinking")?.value !==
                          selection.options?.find((option) => option.id === "thinking")?.value
                      )
                        return yield* new ProviderMaintenanceError({
                          threadId: input.threadId,
                          reason:
                            "Pi could not verify the original effective model and thinking level.",
                        });
                      yield* replacement.exitMaintenance({ requestId: input.requestId });
                    }),
                  );
                }).pipe(
                  Effect.timeout(
                    Math.min(
                      110_000,
                      Math.max(
                        0,
                        reservation.deadlineEpochMs - DateTime.toEpochMillis(yield* DateTime.now),
                      ),
                    ),
                  ),
                ),
              ),
            );
            if (Exit.isSuccess(attempt)) {
              if (DateTime.toEpochMillis(yield* DateTime.now) >= reservation.deadlineEpochMs) {
                yield* record(
                  input,
                  "unknown",
                  "Pi restart crossed its acknowledgment deadline. It was not replayed; new work remains blocked until containment is verified.",
                );
                return yield* new ProviderMaintenanceError({
                  threadId: input.threadId,
                  reason: "Pi restart did not acknowledge within its deadline.",
                });
              }
              yield* record(
                input,
                "succeeded",
                "Pi RPC restarted in the same saved conversation. No prompt was sent. Notebook runtime bindings may have been lost.",
              );
              yield* admission.release(gate);
              return;
            }
            // A failed resume leaves no half-configured replacement for the next message to use.
            const cleanup = replacementOpened
              ? yield* Effect.exit(
                  restore(
                    admission
                      .withOwnerAdmission(
                        gate,
                        sessions.release({
                          providerSessionId: reservation.providerSessionId,
                          reason: "manual_shutdown",
                          requireConfirmedShutdown: true,
                        }),
                      )
                      .pipe(Effect.timeout("65 seconds")),
                  ),
                )
              : !shutdownStarted && barrierAttempted && oldRuntime?.exitMaintenance !== undefined
                ? yield* Effect.exit(
                    restore(
                      oldRuntime
                        .exitMaintenance({ requestId: input.requestId })
                        .pipe(Effect.timeout("5 seconds")),
                    ),
                  )
                : Exit.void;
            const uncertain =
              (shutdownStarted && !oldShutdownConfirmed) ||
              cleanup._tag === "Failure" ||
              // Failed open may have spawned a process before failing to return its runtime.
              // Generic open cleanup does not promise owned-process exit confirmation.
              (replacementOpenAttempted && !replacementOpened) ||
              (shutdownStarted &&
                DateTime.toEpochMillis(yield* DateTime.now) >= reservation.deadlineEpochMs);
            const message = uncertain
              ? "Pi RPC shutdown could not be confirmed. Restart was not retried and new work is blocked. Check the provider and server logs before an explicit retry."
              : "Pi RPC restart failed. No prompt or replacement conversation was sent, and it was not retried. Check the provider and server logs before trying again.";
            yield* record(input, uncertain ? "unknown" : "failed", message);
            if (!uncertain) yield* admission.release(gate);
            return yield* new ProviderMaintenanceError({
              threadId: input.threadId,
              reason: message,
              cause: attempt.cause,
            });
          }),
        ).pipe(
          (effect) => operationLock.withLock(input.requestId, effect),
          Effect.mapError((cause) =>
            isProviderMaintenanceError(cause)
              ? cause
              : new ProviderMaintenanceError({
                  threadId: input.threadId,
                  reason:
                    "Could not record the Pi RPC restart outcome. The operation will not be retried.",
                  cause,
                }),
          ),
        ),
    });
  }),
).pipe(Layer.provide(MaintenanceAdmission.layer));
