# Learning from a reconstructed Grok Bot

## Source boundary and provenance

This note records what was learned from an independent reading of the
unofficial reconstruction published at
<https://github.com/b-nnett/grok-bot-0.18-reconstructed/tree/a9f633e09d49a85829b8236331b9e21f7e612634>.

That repository is **unofficial, incomplete, and unlicensed upstream
material**. It is a third-party reconstruction of a product, not a release by
the product's authors, and it carries no upstream license. **No source code or
prompt text from it was copied into Herdr Bots**, and the mechanism that was
adopted was independently specified against Herdr Bots' own safety contract
(durable state first, authority fencing, no automatic retry, no external
side effect before persistence).

Of the five mechanisms below, only the **unread-work guard** introduced a new
Herdr Bots control surface (`attention.max_unread_terminal_runs`). The other
four already existed here in equivalent form; they are documented as shared
patterns, not as additions.

## Five reusable mechanisms

### 1. Intent-level charters

A bot is defined by a saved, versioned intent (the job charter: repository,
route, prompt, permission profile, verifier, limits) rather than by an
ad-hoc conversation or a one-off command line. Herdr Bots already implements
this as the immutable job snapshot: the config file is executable authority,
every run pins the exact snapshot that authorized it, and the saved prompt —
never a caller-supplied payload — is the only instruction source. The lesson
reinforced: put reviewability and revision fencing in the charter, not in the
run.

### 2. Event payloads as data

Triggering inputs should be typed, minimal identities rather than arbitrary
operator-provided content. Herdr Bots' event intake already follows this: an
event occurrence carries only a validated event ID, no prompt override,
context file, JSON, or shell text. The durable run is authorized exclusively
by the saved charter. The lesson reinforced: events name *when*, the charter
alone decides *what*.

### 3. Visible delivery distinct from private reasoning

What an operator must see (evidence: states, verdicts, run records) is a
different channel from what the agent privately reasons with. Herdr Bots
already separates the read-oriented evidence inbox (`runs`, `show`, `pane`)
from the agent's working transcript inside its isolated worktree. The lesson
reinforced: the inbox is a durable, per-run record; it is never a place to
inject new instructions or to expose the agent's scratch work as authority.

### 4. Lifecycle gates

A run's lifecycle should be a sequence of durable, compare-and-set gates
(accepted → provisioning → starting → running → settled → verifying →
terminal), each persisted before the external effect it guards. Herdr Bots
already implements this contract: no workspace or agent is created before the
run row exists, claims and leases are fenced, terminal states are immutable,
and ambiguous external outcomes fail closed. The lesson reinforced: every
external effect must have a durable gate that a restart can reconcile.

### 5. The unread-work guard

An operator's attention is a finite resource. When a bot keeps producing
finished work that nobody has reviewed, the correct control is to stop
scheduling *more* work rather than to pile unread results higher. Herdr Bots
adopted this as an **opt-in, per-job guard**: `attention.max_unread_terminal_runs`
(1..1000) pauses the job — atomically, inside the same authority-fenced
admission transaction — before another run is admitted once that many
terminal runs are still unread. Nonterminal runs never count merely because
runs begin unread; marking runs read lowers the count but never auto-resumes
the job; only an explicit `herdr-bots resume` does. The pause carries a
durable reason (`unread_terminal_runs`) and timestamp, distinct from a manual
pause, and is visible in `herdr-bots list`. This is the only mechanism from
this study that added a new Herdr Bots control, and its design, constants,
and thresholds were chosen for Herdr Bots independently.
