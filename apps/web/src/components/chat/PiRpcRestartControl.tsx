import { useAtomValue } from "@effect/atom-react";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import {
  CommandId,
  OrchestrationV2DispatchCommandError,
  type EnvironmentId,
  type ThreadId,
} from "@t3tools/contracts";
import { RotateCcwIcon } from "lucide-react";
import * as Schema from "effect/Schema";
import { useRef, useState } from "react";

import { ensureLocalApi } from "../../localApi";
import { randomUUID } from "../../lib/utils";
import { useServerConfigs, useThreadProjection, useThreadShell } from "../../state/entities";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { toastManager } from "../ui/toast";
import { ThreadDetailsControl } from "./ThreadDetailsControl";
import { ThreadDetailsSection } from "./ThreadDetailsSection";

const isDispatchCommandError = Schema.is(OrchestrationV2DispatchCommandError);

/** Both entry points use the saved runtime identity, never the model name. */
export function usePiRpcRestart(environmentId: EnvironmentId | null, threadId: ThreadId | null) {
  const ref =
    environmentId !== null && threadId !== null ? scopeThreadRef(environmentId, threadId) : null;
  const detail = useThreadProjection(ref);
  const shell = useThreadShell(ref);
  const config = useServerConfigs().get(environmentId ?? ("" as EnvironmentId));
  const projection = detail?.projection;
  const providerThread = projection?.providerThreads.find(
    (candidate) => candidate.id === projection.thread.activeProviderThreadId,
  );
  const session = projection?.providerSessions.find(
    (candidate) => candidate.id === providerThread?.providerSessionId,
  );
  const driver =
    providerThread?.driver ??
    config?.providers.find(
      (provider) =>
        provider.instanceId ===
        (shell?.runtime?.providerInstanceId ?? shell?.modelSelection.instanceId),
    )?.driver;
  const isPi = driver === "pi";
  const saved =
    providerThread?.driver === "pi" &&
    session?.driver === "pi" &&
    providerThread.nativeThreadRef !== null &&
    providerThread.nativeThreadRef !== undefined;
  const maintenance = shell?.source.providerMaintenance ?? projection?.thread.providerMaintenance;
  const canOperate = useAtomValue(threadEnvironment.restartPiRpc.permissionAtom(environmentId));
  const dispatch = useAtomCommand(threadEnvironment.restartPiRpc, { reportFailure: false });
  const [submitting, setSubmitting] = useState(false);
  const [requested, setRequested] = useState<{
    environmentId: EnvironmentId;
    threadId: ThreadId;
    commandId: CommandId;
  } | null>(null);
  const [failure, setFailure] = useState<{
    environmentId: EnvironmentId;
    threadId: ThreadId;
    message: string;
  } | null>(null);
  const requestId =
    requested?.environmentId === environmentId && requested.threadId === threadId
      ? requested.commandId
      : null;
  const inFlight = useRef(false);
  const unresolved =
    maintenance?.status === "pending" ||
    maintenance?.status === "running" ||
    maintenance?.status === "unknown" ||
    (requestId !== null && maintenance?.requestId !== requestId);
  const available = saved && canOperate && !submitting && !unresolved;
  const durableStatus =
    maintenance === undefined
      ? null
      : `${maintenance.kind === "pi-rpc-restart" ? "Pi RPC restart" : "Suite reload"}: ${maintenance.status}${maintenance.message ? ` — ${maintenance.message}` : ""}`;
  const requestStatus =
    requestId !== null && maintenance?.requestId !== requestId
      ? "Pi RPC restart requested; waiting for a durable outcome. Acceptance is not completion."
      : durableStatus;
  const localFailure =
    failure?.environmentId === environmentId && failure.threadId === threadId
      ? failure.message
      : null;
  const status = [requestStatus, localFailure].filter(Boolean).join(" ") || null;

  async function restart() {
    if (!available || inFlight.current || environmentId === null || threadId === null || !session)
      return;
    inFlight.current = true;
    setSubmitting(true);
    try {
      const confirmed = await ensureLocalApi().dialogs.confirm(
        "Restart the Pi RPC process? This keeps the same T3 thread, saved native Pi conversation, and model. No prompt will be sent or replayed, no model continuation will run, and no new app run will start. Notebook runtime bindings may be lost. The server refuses restart while busy, with pending requests, background or delegated work, or conflicting lifecycle work. Completion is reported in Thread Details; an unknown outcome must not be retried blindly.",
      );
      if (!confirmed) return;
      setFailure(null);
      const commandId = CommandId.make(randomUUID());
      // Retain identity before transport: a lost response is not a refused operation.
      setRequested({ environmentId, threadId, commandId });
      const result = await dispatch({
        environmentId,
        input: {
          type: "provider-session.restart",
          commandId,
          threadId,
          providerSessionId: session.id,
        },
      });
      if (result._tag === "Failure") throw squashAtomCommandFailure(result);
      toastManager.add({
        type: "info",
        title: "Pi RPC restart requested",
        description: "Acceptance is not completion. Check the durable outcome in Thread Details.",
      });
    } catch (error) {
      if (isDispatchCommandError(error) && error.notAccepted === true) setRequested(null);
      const message = `${error instanceof Error ? error.message : String(error)} Check the durable outcome before attempting again; no automatic retry was made.`;
      setFailure({ environmentId, threadId, message });
      toastManager.add({
        type: "error",
        title: "Pi RPC restart was not confirmed",
        description: message,
      });
    } finally {
      inFlight.current = false;
      setSubmitting(false);
    }
  }
  return {
    isPi,
    knownNonPi: driver !== undefined && !isPi,
    saved,
    available,
    status,
    submitting,
    restart,
  };
}

export function PiRpcRestartControl(props: { environmentId: EnvironmentId; threadId: ThreadId }) {
  const action = usePiRpcRestart(props.environmentId, props.threadId);
  if (!action.saved && action.status === null) return null;
  return (
    <ThreadDetailsSection headingId="thread-details-pi-rpc-heading" title="Pi runtime">
      {action.saved ? (
        <ThreadDetailsControl disabled={!action.available} onClick={() => void action.restart()}>
          <RotateCcwIcon aria-hidden />
          {action.submitting ? "Requesting Pi RPC restart…" : "Restart Pi RPC process"}
        </ThreadDetailsControl>
      ) : null}
      {action.status ? (
        <p role="status" className="px-1.5 text-xs text-muted-foreground">
          {action.status}
        </p>
      ) : null}
    </ThreadDetailsSection>
  );
}
