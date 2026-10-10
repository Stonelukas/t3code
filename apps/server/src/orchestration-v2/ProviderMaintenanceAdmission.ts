import { CommandId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";

export type AdmissionScope =
  | { readonly scope: "thread"; readonly threadId: ThreadId }
  | { readonly scope: "environment"; readonly threadId?: ThreadId };

export type MaintenanceRequest = AdmissionScope & { readonly requestId: CommandId };
export type Reservation = MaintenanceRequest & { readonly reservationId: number };

export class ProviderMaintenanceAdmissionError extends Schema.TaggedError<ProviderMaintenanceAdmissionError>()(
  "ProviderMaintenanceAdmissionError",
  {
    reason: Schema.Literals([
      "reserved",
      "in_flight",
      "stale_ticket",
      "stale_reservation",
      "activity_changed",
      "not_begun",
    ]),
    threadId: Schema.optional(ThreadId),
    requestId: Schema.optional(CommandId),
  },
) {
  override get message(): string {
    return `Provider maintenance admission refused: ${this.reason}.`;
  }
}

export class ProviderMaintenanceAdmission extends Context.Service<
  ProviderMaintenanceAdmission,
  {
    readonly withAdmission: <A, E, R>(
      input: { readonly threadId?: ThreadId; readonly requestId?: CommandId },
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | ProviderMaintenanceAdmissionError, R>;
    readonly reserve: (
      input: MaintenanceRequest,
    ) => Effect.Effect<Reservation, ProviderMaintenanceAdmissionError>;
    readonly begin: (
      reservation: Reservation,
      generation?: number,
    ) => Effect.Effect<void, ProviderMaintenanceAdmissionError>;
    readonly release: (reservation: Reservation) => Effect.Effect<boolean>;
    readonly snapshot: (scope: AdmissionScope) => Effect.Effect<{
      readonly generation: number;
      readonly inFlight: number;
      readonly reservations: ReadonlyArray<Reservation>;
    }>;
    readonly noteNativeActivity: (threadId: ThreadId) => Effect.Effect<void>;
    /** Internal owner command interval, including reservation acquisition. Not for ordinary commands. */
    readonly withMaintenanceAdmission: <A, E, R>(
      request: MaintenanceRequest,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | ProviderMaintenanceAdmissionError, R>;
    /** Only the internal maintenance owner uses this after begin; a request id alone is not authority. */
    readonly withOwnerAdmission: <A, E, R>(
      reservation: Reservation,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | ProviderMaintenanceAdmissionError, R>;
    /** Internal checkpoint drain only, before begin. Does not admit nested commands or runtime mutations. */
    readonly withDrainAdmission: <A, E, R>(
      reservation: Reservation,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | ProviderMaintenanceAdmissionError, R>;
  }
>()("t3/orchestration-v2/ProviderMaintenanceAdmission") {}

type Ticket = {
  readonly scope: AdmissionScope;
  readonly requestId: CommandId | undefined;
  readonly owner: Reservation | undefined;
  readonly maintenance: boolean;
};

// Private, inherited context is a capability only while its exact ticket is still in the Ref.
const CurrentTicket = Context.Reference<Ticket | undefined>(
  "t3/orchestration-v2/ProviderMaintenanceAdmission/CurrentTicket",
  { defaultValue: () => undefined },
);

/** Service-owned daemons outlive their spawning mutation. Their later mutations take fresh tickets. */
export const freshAdmissionContext = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.provideService(effect, CurrentTicket, undefined);

type HeldReservation = {
  readonly reservation: Reservation;
  readonly begun: boolean;
  readonly excludedTicket: Ticket | undefined;
};
type State = {
  readonly nextReservationId: number;
  readonly clock: number;
  readonly environmentActivity: number;
  readonly threads: ReadonlyMap<ThreadId, number>;
  readonly tickets: ReadonlyMap<Ticket, number>;
  readonly reservations: ReadonlyArray<HeldReservation>;
};

const overlaps = (a: AdmissionScope, b: AdmissionScope) =>
  a.scope === "environment" || b.scope === "environment" || a.threadId === b.threadId;
const covers = (a: AdmissionScope, b: AdmissionScope) =>
  a.scope === "environment" || (b.scope === "thread" && a.threadId === b.threadId);
const matches = (a: Reservation, b: Reservation) =>
  a.reservationId === b.reservationId &&
  a.requestId === b.requestId &&
  a.scope === b.scope &&
  a.threadId === b.threadId;
const generation = (state: State, scope: AdmissionScope) =>
  scope.scope === "environment"
    ? state.clock
    : Math.max(state.environmentActivity, state.threads.get(scope.threadId) ?? 0);
const activity = (state: State, scope: AdmissionScope): State => {
  const clock = state.clock + 1;
  return scope.scope === "environment"
    ? { ...state, clock, environmentActivity: clock }
    : { ...state, clock, threads: new Map(state.threads).set(scope.threadId, clock) };
};

// No provider work runs inside Ref.modify. This is only an in-memory gate; the
// durable owner is responsible for inspection, deadlines, recovery and release.
const make = Effect.gen(function* () {
  const state = yield* Ref.make<State>({
    nextReservationId: 1,
    clock: 0,
    environmentActivity: 0,
    threads: new Map(),
    tickets: new Map(),
    reservations: [],
  });
  const fail = (
    reason: ProviderMaintenanceAdmissionError["reason"],
    input: { readonly threadId?: ThreadId; readonly requestId?: CommandId },
  ) => Effect.fail(new ProviderMaintenanceAdmissionError({ reason, ...input }));

  const acquire = Effect.fnUntraced(function* (
    scope: AdmissionScope,
    requestId: CommandId | undefined,
    owner?: Reservation,
    maintenance = false,
    drain = false,
  ) {
    const inherited = yield* CurrentTicket;
    return yield* Ref.modify(
      state,
      (current): readonly [Effect.Effect<Ticket, ProviderMaintenanceAdmissionError>, State] => {
        const input = {
          ...(scope.threadId === undefined ? {} : { threadId: scope.threadId }),
          ...(requestId === undefined ? {} : { requestId }),
        };
        if (inherited !== undefined && !current.tickets.has(inherited)) {
          return [fail("stale_ticket", input), current];
        }
        const heldOwner =
          owner === undefined
            ? undefined
            : current.reservations.find((held) => matches(held.reservation, owner));
        if (owner !== undefined && heldOwner === undefined)
          return [fail("stale_reservation", input), current];
        if (heldOwner !== undefined && (drain ? heldOwner.begun : !heldOwner.begun))
          return [fail(drain ? "reserved" : "not_begun", input), current];
        // Drain authority is deliberately not inherited by ordinary admission.
        const effectiveOwner = drain ? undefined : (owner ?? inherited?.owner);
        if (
          effectiveOwner !== undefined &&
          !current.reservations.some(
            (held) => held.begun && matches(held.reservation, effectiveOwner),
          )
        ) {
          return [fail("stale_reservation", input), current];
        }
        if (
          current.reservations.some(
            (held) =>
              overlaps(held.reservation, scope) &&
              !(drain && owner !== undefined && matches(held.reservation, owner)) &&
              (effectiveOwner === undefined || !matches(held.reservation, effectiveOwner)),
          )
        ) {
          return [fail("reserved", input), current];
        }
        // Ordinary nesting cannot widen the protected scope or mint owner authority.
        if (
          inherited !== undefined &&
          covers(inherited.scope, scope) &&
          (!maintenance || (inherited.maintenance && inherited.requestId === requestId)) &&
          (owner === undefined ||
            (inherited.owner !== undefined && matches(inherited.owner, owner)))
        ) {
          // Retain the same ticket for each protected interval, including forks
          // that enter before the outer interval finishes.
          const tickets = new Map(current.tickets).set(
            inherited,
            (current.tickets.get(inherited) ?? 0) + 1,
          );
          return [Effect.succeed(inherited), activity({ ...current, tickets }, inherited.scope)];
        }
        const ticket: Ticket = { scope, requestId, owner: effectiveOwner, maintenance };
        return [
          Effect.succeed(ticket),
          activity({ ...current, tickets: new Map(current.tickets).set(ticket, 1) }, scope),
        ];
      },
    ).pipe(Effect.flatten);
  });
  const protect = <A, E, R>(
    scope: AdmissionScope,
    requestId: CommandId | undefined,
    effect: Effect.Effect<A, E, R>,
    owner?: Reservation,
    maintenance = false,
    drain = false,
  ) =>
    Effect.acquireUseRelease(
      acquire(scope, requestId, owner, maintenance, drain),
      (ticket) => Effect.provideService(effect, CurrentTicket, ticket),
      (ticket) =>
        Ref.update(state, (current) => {
          const tickets = new Map(current.tickets);
          const remaining = (tickets.get(ticket) ?? 1) - 1;
          if (remaining === 0) tickets.delete(ticket);
          else tickets.set(ticket, remaining);
          return activity({ ...current, tickets }, ticket.scope);
        }),
    );

  return ProviderMaintenanceAdmission.of({
    withAdmission: (input, effect) =>
      protect(
        input.threadId === undefined
          ? { scope: "environment" }
          : { scope: "thread", threadId: input.threadId },
        input.requestId,
        effect,
      ),
    withMaintenanceAdmission: (request, effect) =>
      protect(request, request.requestId, effect, undefined, true),
    withOwnerAdmission: (reservation, effect) =>
      protect(reservation, reservation.requestId, effect, reservation, true),
    withDrainAdmission: (reservation, effect) =>
      protect(reservation, reservation.requestId, effect, reservation, false, true),
    reserve: Effect.fnUntraced(function* (input) {
      const inherited = yield* CurrentTicket;
      return yield* Ref.modify(
        state,
        (
          current,
        ): readonly [Effect.Effect<Reservation, ProviderMaintenanceAdmissionError>, State] => {
          if (inherited !== undefined && !current.tickets.has(inherited))
            return [fail("stale_ticket", input), current];
          if (current.reservations.some((held) => overlaps(held.reservation, input)))
            return [fail("reserved", input), current];
          const excludedTicket =
            inherited !== undefined &&
            inherited.maintenance &&
            current.tickets.get(inherited) === 1 &&
            inherited.requestId === input.requestId &&
            covers(input, inherited.scope)
              ? inherited
              : undefined;
          if (
            [...current.tickets.keys()].some(
              (ticket) => ticket !== excludedTicket && overlaps(ticket.scope, input),
            )
          )
            return [fail("in_flight", input), current];
          const reservation: Reservation = { ...input, reservationId: current.nextReservationId };
          return [
            Effect.succeed(reservation),
            {
              ...current,
              nextReservationId: current.nextReservationId + 1,
              reservations: [
                ...current.reservations,
                { reservation, begun: false, excludedTicket },
              ],
            },
          ];
        },
      ).pipe(Effect.flatten);
    }),
    begin: Effect.fnUntraced(function* (reservation, capturedGeneration) {
      const inherited = yield* CurrentTicket;
      return yield* Ref.modify(
        state,
        (current): readonly [Effect.Effect<void, ProviderMaintenanceAdmissionError>, State] => {
          const held = current.reservations.find((held) => matches(held.reservation, reservation));
          if (held === undefined) return [fail("stale_reservation", reservation), current];
          if (inherited !== undefined && !current.tickets.has(inherited))
            return [fail("stale_ticket", reservation), current];
          if (
            capturedGeneration !== undefined &&
            capturedGeneration !== generation(current, reservation)
          )
            return [fail("activity_changed", reservation), current];
          if (
            [...current.tickets.keys()].some(
              (ticket) =>
                overlaps(ticket.scope, reservation) &&
                !(
                  ticket === inherited &&
                  ticket === held.excludedTicket &&
                  current.tickets.get(ticket) === 1
                ),
            )
          )
            return [fail("in_flight", reservation), current];
          return [
            Effect.void,
            {
              ...current,
              reservations: current.reservations.map((entry) =>
                entry === held ? { ...entry, begun: true } : entry,
              ),
            },
          ];
        },
      ).pipe(Effect.flatten);
    }),
    release: (reservation) =>
      Ref.modify(state, (current) => {
        const reservations = current.reservations.filter(
          (held) => !matches(held.reservation, reservation),
        );
        return [reservations.length !== current.reservations.length, { ...current, reservations }];
      }),
    snapshot: (scope) =>
      Ref.get(state).pipe(
        Effect.map((current) => ({
          generation: generation(current, scope),
          inFlight: [...current.tickets.keys()].filter((ticket) => overlaps(ticket.scope, scope))
            .length,
          reservations: current.reservations
            .filter((held) => overlaps(held.reservation, scope))
            .map((held) => ({ ...held.reservation })),
        })),
      ),
    noteNativeActivity: (threadId) =>
      Ref.update(state, (current) => activity(current, { scope: "thread", threadId })),
  });
});

export const layer = Layer.effect(ProviderMaintenanceAdmission, make);
