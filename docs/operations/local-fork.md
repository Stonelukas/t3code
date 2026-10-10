# Stonelukas local fork maintenance

This fork carries small local T3 improvements while retaining upstream updates.
Maintenance is coordinated by a dedicated T3 thread; durable source and operating
rules live in the repository, not only in that conversation.

## Source ownership

- Fork: `https://github.com/Stonelukas/t3code` (`origin`).
- Upstream: `https://github.com/pingdotgg/t3code` (`upstream`).
- Local integration branch: `stonelukas/t3-suite`.
- Initial base: `v0.0.46-nightly.20261009.2873`, commit
  `ec80933ac8cd02fec5c97b342462ccc9567cdb1e`. This is historical source
  provenance, not the currently installed server version.
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

Merge remains owner-controlled. Obtain separate authorization for CI or branch
protection changes, deployment automation, and optional upstream PRs. Track active
implementation work and its decisions in the owning issue or project item, not
this operating guide.

## Preparation is not deployment

Building/testing a candidate does not authorize replacing the installed server or
desktop application, restarting the service, modifying live settings, migrating
the database, or changing upstream auto-update behavior. Establish the exact
artifact and installation path, compatible clients, explicit cutover and a retained
known-good rollback artifact before any later authorized deployment.

Coordinate any cutover with the live service owner. Preserve the live T3 home,
environment identity, existing threads and private credentials. Do not start a
second server against live state or copy credentials into this fork. Verify how
upstream auto-updates interact with a custom build before claiming that an
installation will retain local changes across updates.
