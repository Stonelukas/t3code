# Stonelukas local fork maintenance

This fork carries small local T3 improvements while retaining upstream updates.
Maintenance is coordinated by a dedicated T3 thread; durable source and operating
rules live in the repository, not only in that conversation.

## Source ownership

- Fork: `https://github.com/Stonelukas/t3code` (`origin`).
- Upstream: `https://github.com/pingdotgg/t3code` (`upstream`).
- Local integration branch: `stonelukas/t3-suite`.
- Initial base: `v0.0.46-nightly.20261009.2873`, commit
  `ec80933ac8cd02fec5c97b342462ccc9567cdb1e`, matching the current server version.
- Dedicated checkout: `/home/stonelukas/Projects/t3code`. Do not repurpose the
  older checkout at `/home/stonelukas/Dev/agent-tools/t3code` or discard its edits.

Keep local changes small and separable. Record the upstream base and local commit
identities when preparing an update. Do not silently switch the integration branch
to a new release channel. Fetching upstream is not installing it.

## Maintenance boundary

The owner selected a lightweight GitHub fork and T3 maintenance thread. Do not add
T3 as a Dotctl managed-fork product or reproduce that updater's validation matrix.
Do not create recurring work unless explicitly requested.

For an upstream update, inspect changes, prepare an integration candidate, resolve
conflicts and run the smallest relevant existing checks. Follow the current
`AGENTS.md` and repository-defined build/release commands. Do not assume the
commands or constraints from an older checkout still apply. Preserve unrelated
work; never force-push, discard patches or publish PRs without exact permission.
Commits and pushes of local modifications need their applicable owner approval.

## Fork review workflow

Local fixes and upstream integrations go through topic branches and PRs in
`Stonelukas/t3code`, targeting `stonelukas/t3-suite`. Do not push local
modifications directly to the integration branch. Run the relevant focused
checks and obtain owner review before merge; keep optional upstream contribution
PRs separate from fork-local review.

The owner has authorized topic-branch commits, pushes and fork-local PRs for the
bounded questionnaire, RPC session refresh and necessary Suite host integration
work, including a separate latest-nightly integration PR first. Merge remains
owner-controlled. This does not authorize configuring CI or branch protection,
adding deployment automation, or creating optional upstream PRs.

## Preparation is not deployment

Building/testing a candidate does not authorize replacing the installed server or
desktop application, restarting the service, modifying live settings, migrating
the database, or changing upstream auto-update behavior. Establish the exact
artifact and installation path, compatible clients, explicit cutover and a retained
known-good rollback artifact before any later authorized deployment.

The current live service/environment belongs to the separate service owner:
`ec04a820-92fc-4586-badf-9289cd89a0c8` (Host T3 Code Across Your Tailnet).
Preserve `/home/stonelukas/.t3`, Cachy environment identity
`d701cb67-8fc3-41f9-910d-fd9d996165a7`, existing threads and private credentials.
Do not start a second server against live state or copy credentials into this fork.
The interaction between upstream auto-updates and a custom installed build remains
unresolved; do not claim the installation is protected until that is verified.

## Initial integration concerns

Requirements and open decisions remain owned by
`/home/stonelukas/Projects/pi-extensions/brainstorms/t3-backend-readiness/prd.md`
and `/home/stonelukas/dotfiles/docs/feature-backlog.md` (FR-023 and FR-024).
Do not duplicate their execution state here.

- Questionnaire: retain useful context, display the question once, separate
  option descriptions, and avoid a detached comment workflow. The current Pi
  string-only dialog mapping and T3 whitespace/title handling need a coordinated
  design. The illustrative preview is not final design approval.
- Shared agent extension reload belongs in the Suite at
  `/home/stonelukas/Projects/pi-extensions`, supporting both T3 Code and Pi TUI.
  This fork owns only necessary T3 host/run/continuation/catalog integration,
  not a competing reload engine. Audit public Pi 1.0.4 command-context `reload()`
  and `reloadWithContext()` before considering a new primitive. The existing
  `execute-command.ts` continuation repair is externally owned and in progress;
  coordinate reuse rather than duplicating it. Do not load that legacy extension
  wholesale or reintroduce its terminal `/answer` workflow. Suite source edits
  from this thread require an explicit assignment; main Pi activation is excluded.
- User-facing RPC session refresh is a distinct T3 requirement: preserve the T3
  conversation while refreshing the `pi --rpc` session. In-process reload versus
  controlled process restart/resume remains to verify. Browser refresh, provider
  switching and resource discovery are not session-refresh success. Busy/pending
  handling, request invalidation, Notebook state disclosure, catalog alignment
  and failure recovery need explicit behavior decisions. Extension reload does
  not upgrade Pi core or establish that PCC/runtime changes can be hot-reloaded.
  Neither this requirement nor historical tests authorize a live reload/restart.
- Relevant tool-output visibility: the Suite owner investigates its own
  output-redaction path. Do not disable global credential filtering or silently
  request unfiltered approvals as a workaround.

The owner has authorized bounded T3 source implementation and focused verification
in this checkout, with repository dependency installation when needed. Start with
contract-preserving questionnaire readability fixes. Material unresolved lifecycle
or cross-repository protocol decisions still require owner input before code.
Do not edit Suite or dotfiles source from this thread without explicit assignment.

Browser/server launches, live reloads/restarts, package/runtime installation into
the live environment, secret/data copies, merge and deployment are not authorized
by this source/PR work. Main Pi activation remains excluded. Keep candidate
preparation and publishing separate from any later authorized cutover.
