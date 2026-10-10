import { assert, it } from "@effect/vitest";
import {
  CommandId,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ThreadProjection,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { CursorProviderCapabilitiesV2 } from "@t3tools/provider-cursor/testing";
import { GrokProviderCapabilitiesV2 } from "@t3tools/provider-grok/testing";
import * as CommandPolicy from "./CommandPolicy.ts";
import * as ProviderMaintenanceAdmission from "./ProviderMaintenanceAdmission.ts";

const commandId = CommandId.make("command-policy-test");
const threadId = ThreadId.make("command-policy-thread");
const activeRunId = RunId.make("command-policy-active-run");

const baseCapabilities: OrchestrationV2ProviderCapabilities = CodexProviderCapabilitiesV2;

function capabilities(
  override: (current: OrchestrationV2ProviderCapabilities) => OrchestrationV2ProviderCapabilities,
): OrchestrationV2ProviderCapabilities {
  return override(baseCapabilities);
}

function dispatchProjection(
  sessionCapabilities?: OrchestrationV2ProviderCapabilities,
): OrchestrationV2ThreadProjection {
  const providerThreadId = ProviderThreadId.make("command-policy-provider-thread");
  const providerSessionId = ProviderSessionId.make("command-policy-provider-session");
  return {
    runs:
      sessionCapabilities === undefined
        ? []
        : [{ id: activeRunId, status: "running", providerThreadId }],
    providerThreads:
      sessionCapabilities === undefined ? [] : [{ id: providerThreadId, providerSessionId }],
    providerSessions:
      sessionCapabilities === undefined
        ? []
        : [{ id: providerSessionId, capabilities: sessionCapabilities }],
  } as unknown as OrchestrationV2ThreadProjection;
}

it("resolves automatic message delivery from authoritative provider capabilities", () => {
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "start_immediately" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(baseCapabilities),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "steer_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(
        capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsQueuedMessages: true,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      ),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "queue_after_active" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(
        capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsQueuedMessages: false,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      ),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "restart_active", targetRunId: activeRunId },
  );
});

it.each(["preparing", "starting"] as const)(
  "queues an automatic message while the handoff run is %s",
  (status) => {
    const projection = dispatchProjection(baseCapabilities);
    assert.deepEqual(
      CommandPolicy.resolveMessageDispatchIntent(
        { ...projection, runs: projection.runs.map((run) => ({ ...run, status })) },
        { type: "start_immediately" },
        "auto",
      ),
      { type: "queue_after_active" },
    );
  },
);

it("targets the latest active run for explicit steer and restart intent", () => {
  const projection = dispatchProjection(baseCapabilities);
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(projection, { type: "start_immediately" }, "steer"),
    { type: "steer_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      projection,
      { type: "start_immediately" },
      "restart",
    ),
    { type: "restart_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(),
      { type: "start_immediately" },
      "steer",
    ),
    { type: "start_immediately" },
  );
});

const layer = it.layer(CommandPolicy.layer);

layer("CommandPolicyV2", (it) => {
  it.effect("prefers direct active steering when the provider supports it", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: baseCapabilities,
      });

      assert.equal(result, "active_steering");
    }),
  );

  it.effect("uses interrupt-and-restart steering when direct steering is unavailable", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsInterrupt: true,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      });

      assert.equal(result, "interrupt_restart");
    }),
  );

  it.effect("uses interrupt-and-restart steering for Grok ACP", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("grok"),
        capabilities: GrokProviderCapabilitiesV2,
      });

      assert.equal(result, "interrupt_restart");
    }),
  );

  it.effect("honors an explicit interrupt-and-restart request", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: CodexProviderCapabilitiesV2,
        forceRestart: true,
      });

      assert.equal(result, "interrupt_restart");
    }),
  );

  it.effect("reports the actually missing capability across the steering matrix", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;
      const providerInstanceId = ProviderInstanceId.make("codex");
      const fallbackDetail =
        "providerInstanceId cannot steer active turns directly or by interrupt-and-restart";
      const forcedRestartDetail =
        "providerInstanceId cannot satisfy a required interrupt-and-restart";

      const cases: ReadonlyArray<{
        readonly forceRestart: boolean;
        readonly supportsActiveSteering: boolean;
        readonly supportsInterrupt: boolean;
        readonly supportsSteeringByInterruptRestart: boolean;
        readonly expected:
          | { readonly type: "policy"; readonly value: CommandPolicy.SteeringExecutionPolicyV2 }
          | {
              readonly type: "error";
              readonly capability: CommandPolicy.CommandPolicyCapability;
              readonly detail: string;
            };
      }> = [
        // Ordinary steering prefers direct active steering whenever available.
        {
          forceRestart: false,
          supportsActiveSteering: true,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: false,
          expected: { type: "policy", value: "active_steering" },
        },
        {
          forceRestart: false,
          supportsActiveSteering: true,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: true,
          expected: { type: "policy", value: "active_steering" },
        },
        {
          forceRestart: false,
          supportsActiveSteering: true,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: false,
          expected: { type: "policy", value: "active_steering" },
        },
        {
          forceRestart: false,
          supportsActiveSteering: true,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: true,
          expected: { type: "policy", value: "active_steering" },
        },
        // Ordinary steering falls back to interrupt-and-restart, then to a
        // typed error naming the capability that is actually missing.
        {
          forceRestart: false,
          supportsActiveSteering: false,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: true,
          expected: { type: "policy", value: "interrupt_restart" },
        },
        {
          forceRestart: false,
          supportsActiveSteering: false,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: false,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: fallbackDetail,
          },
        },
        {
          forceRestart: false,
          supportsActiveSteering: false,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: true,
          expected: { type: "error", capability: "active_steering", detail: fallbackDetail },
        },
        {
          forceRestart: false,
          supportsActiveSteering: false,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: false,
          expected: { type: "error", capability: "active_steering", detail: fallbackDetail },
        },
        // A forced restart skips direct steering and only succeeds via
        // interrupt-and-restart; every other combination must report that
        // capability instead of claiming live steering is unsupported.
        {
          forceRestart: true,
          supportsActiveSteering: true,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: true,
          expected: { type: "policy", value: "interrupt_restart" },
        },
        {
          forceRestart: true,
          supportsActiveSteering: false,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: true,
          expected: { type: "policy", value: "interrupt_restart" },
        },
        {
          forceRestart: true,
          supportsActiveSteering: true,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: false,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: forcedRestartDetail,
          },
        },
        {
          forceRestart: true,
          supportsActiveSteering: false,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: false,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: forcedRestartDetail,
          },
        },
        {
          forceRestart: true,
          supportsActiveSteering: true,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: true,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: forcedRestartDetail,
          },
        },
        {
          forceRestart: true,
          supportsActiveSteering: false,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: true,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: forcedRestartDetail,
          },
        },
        {
          forceRestart: true,
          supportsActiveSteering: true,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: false,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: forcedRestartDetail,
          },
        },
        {
          forceRestart: true,
          supportsActiveSteering: false,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: false,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: forcedRestartDetail,
          },
        },
      ];

      for (const entry of cases) {
        const decision = policy.decideSteeringExecution({
          commandId,
          threadId,
          providerInstanceId,
          forceRestart: entry.forceRestart,
          capabilities: capabilities((current) => ({
            ...current,
            turns: {
              ...current.turns,
              supportsActiveSteering: entry.supportsActiveSteering,
              supportsInterrupt: entry.supportsInterrupt,
              supportsSteeringByInterruptRestart: entry.supportsSteeringByInterruptRestart,
            },
          })),
        });

        if (entry.expected.type === "policy") {
          assert.equal(yield* decision, entry.expected.value);
          continue;
        }

        const error = yield* decision.pipe(Effect.flip);
        assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
        assert.equal(error.commandId, commandId);
        assert.equal(error.threadId, threadId);
        assert.equal(error.providerInstanceId, providerInstanceId);
        assert.equal(error.capability, entry.expected.capability);
        assert.equal(error.detail, entry.expected.detail);
      }
    }),
  );

  it.effect("returns typed capability errors for unsupported active steering", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const error = yield* policy
        .decideSteeringExecution({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          capabilities: capabilities((current) => ({
            ...current,
            turns: {
              ...current.turns,
              supportsActiveSteering: false,
              supportsInterrupt: false,
              supportsSteeringByInterruptRestart: false,
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "active_steering");
    }),
  );

  it.effect("guards native fork behind fork and identity capabilities", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const error = yield* policy
        .ensureNativeFork({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          fromSpecificTurn: true,
          capabilities: capabilities((current) => ({
            ...current,
            identity: {
              ...current.identity,
              nativeThreadIds: "weak",
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "native_fork");
    }),
  );

  it.effect("uses a native fork when the provider supports the requested source point", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideForkExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: CodexProviderCapabilitiesV2,
        sameProvider: true,
        hasStrongNativeSource: true,
        sourceRunStatus: "completed",
        fromSpecificTurn: true,
      });

      assert.equal(result, "native_fork");
    }),
  );

  it.effect("falls back to portable context when Cursor cannot fork natively", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideForkExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("cursor"),
        capabilities: CursorProviderCapabilitiesV2,
        sameProvider: true,
        hasStrongNativeSource: true,
        sourceRunStatus: "completed",
        fromSpecificTurn: true,
      });

      assert.equal(result, "portable_context");
    }),
  );

  it.effect("falls back to portable context when Grok ACP cannot fork natively", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideForkExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("grok"),
        capabilities: GrokProviderCapabilitiesV2,
        sameProvider: true,
        hasStrongNativeSource: true,
        sourceRunStatus: "completed",
        fromSpecificTurn: true,
      });

      assert.equal(result, "portable_context");
    }),
  );

  it.effect("returns a typed error when neither native nor portable fork is available", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const error = yield* policy
        .decideForkExecution({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("cursor"),
          capabilities: capabilities((current) => ({
            ...current,
            threads: {
              ...current.threads,
              canForkThread: false,
            },
            context: {
              ...current.context,
              canConsumeHandoffSummaries: false,
            },
          })),
          sameProvider: true,
          hasStrongNativeSource: true,
          sourceRunStatus: "completed",
          fromSpecificTurn: true,
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "context_handoff");
    }),
  );

  it.effect("guards rollback behind provider rollback snapshot support", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const error = yield* policy
        .ensureRollback({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          capabilities: capabilities((current) => ({
            ...current,
            checkpointing: {
              ...current.checkpointing,
              providerRollbackReturnsSnapshot: false,
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "rollback_snapshot");
    }),
  );

  it.effect("guards fork-delta handoff behind context handoff capabilities", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const error = yield* policy
        .ensureContextHandoff({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          strategy: "fork_delta_context",
          capabilities: capabilities((current) => ({
            ...current,
            context: {
              ...current.context,
              supportsDeltaHandoff: false,
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "context_handoff");
    }),
  );

  it.effect("guards queued turns behind queued-message support", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const error = yield* policy
        .ensureQueuedMessages({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          capabilities: capabilities((current) => ({
            ...current,
            turns: {
              ...current.turns,
              supportsQueuedMessages: false,
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "queued_messages");
    }),
  );
});

it.layer(ProviderMaintenanceAdmission.layer)("ProviderMaintenanceAdmission", (it) => {
  const otherThread = ThreadId.make("maintenance-other-thread");
  const request = { scope: "environment", threadId, requestId: commandId } as const;

  it.effect(
    "refuses a reservation while an admitted commit is in flight and releases on completion",
    () =>
      Effect.gen(function* () {
        const gate = yield* ProviderMaintenanceAdmission.ProviderMaintenanceAdmission;
        const entered = yield* Deferred.make<void>();
        const commit = yield* Deferred.make<void>();
        const fiber = yield* gate
          .withAdmission(
            { threadId },
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(commit);
            }),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        assert.equal((yield* gate.reserve(request).pipe(Effect.flip)).reason, "in_flight");
        assert.equal((yield* gate.snapshot(request)).inFlight, 1);
        yield* Deferred.succeed(commit, undefined);
        yield* Fiber.join(fiber);
        const reservation = yield* gate.reserve(request);
        yield* gate.release(reservation);
      }),
  );

  it.effect("environment reservations reject other threads and matching ordinary request ids", () =>
    Effect.gen(function* () {
      const gate = yield* ProviderMaintenanceAdmission.ProviderMaintenanceAdmission;
      const reservation = yield* gate.reserve({
        scope: "environment",
        threadId,
        requestId: commandId,
      });
      yield* gate.begin(reservation);
      assert.equal(
        (yield* gate.withAdmission({ threadId: otherThread }, Effect.void).pipe(Effect.flip))
          .reason,
        "reserved",
      );
      assert.equal(
        (yield* gate
          .withAdmission({ threadId, requestId: commandId }, Effect.void)
          .pipe(Effect.flip)).reason,
        "reserved",
      );
      yield* gate.release(reservation);
    }),
  );

  it.effect("native activity flows through a closed gate and invalidates captured inspection", () =>
    Effect.gen(function* () {
      const gate = yield* ProviderMaintenanceAdmission.ProviderMaintenanceAdmission;
      const reservation = yield* gate.reserve({ scope: "environment", requestId: commandId });
      const snapshot = yield* gate.snapshot(reservation);
      yield* gate.noteNativeActivity(otherThread);
      assert.equal(
        (yield* gate.begin(reservation, snapshot.generation).pipe(Effect.flip)).reason,
        "activity_changed",
      );
      yield* gate.begin(reservation, (yield* gate.snapshot(reservation)).generation);
      yield* gate.release(reservation);
    }),
  );

  it.effect("release compares the exact reservation, including when a request id is reused", () =>
    Effect.gen(function* () {
      const gate = yield* ProviderMaintenanceAdmission.ProviderMaintenanceAdmission;
      const first = yield* gate.reserve(request);
      assert.isTrue(yield* gate.release(first));
      const second = yield* gate.reserve(request);
      assert.isFalse(yield* gate.release(first));
      assert.isFalse(yield* gate.release({ ...second, requestId: CommandId.make("wrong-owner") }));
      assert.equal((yield* gate.begin(first).pipe(Effect.flip)).reason, "stale_reservation");

      assert.equal(
        (yield* gate.withAdmission({ threadId }, Effect.void).pipe(Effect.flip)).reason,
        "reserved",
      );
      yield* gate.release(second);
    }),
  );

  it.effect("pending drain remains in flight and cannot admit commands or outlive begin", () =>
    Effect.gen(function* () {
      const gate = yield* ProviderMaintenanceAdmission.ProviderMaintenanceAdmission;
      const reservation = yield* gate.reserve({ ...request, scope: "environment" });
      const entered = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<void>();
      const fiber = yield* gate
        .withDrainAdmission(
          reservation,
          Effect.gen(function* () {
            assert.equal((yield* gate.snapshot(reservation)).inFlight, 1);
            assert.equal(
              (yield* gate
                .withAdmission({ threadId, requestId: commandId }, Effect.void)
                .pipe(Effect.flip)).reason,
              "reserved",
            );
            assert.equal(
              (yield* gate.withAdmission({ threadId: otherThread }, Effect.void).pipe(Effect.flip))
                .reason,
              "reserved",
            );
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(finish);
          }),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      assert.equal((yield* gate.begin(reservation).pipe(Effect.flip)).reason, "in_flight");
      yield* Deferred.succeed(finish, undefined);
      yield* Fiber.join(fiber);
      yield* gate.begin(reservation);
      assert.equal(
        (yield* gate.withDrainAdmission(reservation, Effect.void).pipe(Effect.flip)).reason,
        "reserved",
      );
      yield* gate.release(reservation);
      const replacement = yield* gate.reserve({ ...request, scope: "environment" });
      assert.equal(
        (yield* gate.withDrainAdmission(reservation, Effect.void).pipe(Effect.flip)).reason,
        "stale_reservation",
      );
      assert.equal(
        (yield* gate
          .withDrainAdmission({ ...replacement, threadId: otherThread }, Effect.void)
          .pipe(Effect.flip)).reason,
        "stale_reservation",
      );
      yield* gate.release(replacement);
    }),
  );

  it.effect(
    "ordinary nesting retains one ticket and does not gain authority from a matching id",
    () =>
      Effect.gen(function* () {
        const gate = yield* ProviderMaintenanceAdmission.ProviderMaintenanceAdmission;
        yield* gate.withAdmission(
          { threadId, requestId: commandId },
          gate.withAdmission(
            { threadId },
            Effect.gen(function* () {
              assert.equal((yield* gate.snapshot(request)).inFlight, 1);
              assert.equal((yield* gate.reserve(request).pipe(Effect.flip)).reason, "in_flight");
            }),
          ),
        );
        assert.equal((yield* gate.snapshot(request)).inFlight, 0);
      }),
  );

  it.effect(
    "only an explicit reservation interval can exclude its exact ticket without admitting commands",
    () =>
      Effect.gen(function* () {
        const gate = yield* ProviderMaintenanceAdmission.ProviderMaintenanceAdmission;
        yield* gate.withMaintenanceAdmission(
          request,
          Effect.gen(function* () {
            assert.equal(
              (yield* gate
                .reserve({ ...request, requestId: CommandId.make("not-owner") })
                .pipe(Effect.flip)).reason,
              "in_flight",
            );
            const reservation = yield* gate.reserve(request);
            yield* gate.begin(reservation, (yield* gate.snapshot(request)).generation);
            assert.equal(
              (yield* gate
                .withAdmission({ threadId, requestId: commandId }, Effect.void)
                .pipe(Effect.flip)).reason,
              "reserved",
            );
            yield* gate.release(reservation);
          }),
        );
      }),
  );

  it.effect("releases tickets on failure and interruption", () =>
    Effect.gen(function* () {
      const gate = yield* ProviderMaintenanceAdmission.ProviderMaintenanceAdmission;
      yield* gate.withAdmission({ threadId }, Effect.fail("failed")).pipe(Effect.flip);
      const entered = yield* Deferred.make<void>();
      const fiber = yield* gate
        .withAdmission(
          { threadId },
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      assert.equal((yield* gate.snapshot(request)).inFlight, 0);
    }),
  );

  it.effect(
    "an already entered nested fork retains its ticket until its protected interval ends",
    () =>
      Effect.gen(function* () {
        const gate = yield* ProviderMaintenanceAdmission.ProviderMaintenanceAdmission;
        const entered = yield* Deferred.make<void>();
        const finish = yield* Deferred.make<void>();
        const fork = yield* gate.withAdmission(
          { threadId },
          Effect.gen(function* () {
            const child = yield* gate
              .withAdmission(
                { threadId },
                Effect.gen(function* () {
                  yield* Deferred.succeed(entered, undefined);
                  yield* Deferred.await(finish);
                }),
              )
              .pipe(Effect.forkChild);
            yield* Deferred.await(entered);
            return child;
          }),
        );
        assert.equal((yield* gate.snapshot(request)).inFlight, 1);
        assert.equal((yield* gate.reserve(request).pipe(Effect.flip)).reason, "in_flight");
        yield* Deferred.succeed(finish, undefined);
        yield* Fiber.join(fork);
        assert.equal((yield* gate.snapshot(request)).inFlight, 0);
      }),
  );

  it.effect(
    "a fork retaining a released ticket fails closed rather than bypassing a newer gate",
    () =>
      Effect.gen(function* () {
        const gate = yield* ProviderMaintenanceAdmission.ProviderMaintenanceAdmission;
        const resume = yield* Deferred.make<void>();
        const fork = yield* gate.withAdmission(
          { threadId },
          Deferred.await(resume).pipe(
            Effect.andThen(gate.withAdmission({ threadId }, Effect.void)),
            Effect.flip,
            Effect.forkChild,
          ),
        );
        const reservation = yield* gate.reserve(request);
        yield* Deferred.succeed(resume, undefined);
        assert.equal((yield* Fiber.join(fork)).reason, "stale_ticket");
        yield* gate.release(reservation);
      }),
  );
});
