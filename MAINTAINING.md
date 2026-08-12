# Maintaining this fork

This fork does not carry a pile of local commits on a long-lived branch. It carries
a **patch stack**: one branch per feature, and an integration branch that is
*regenerated* from them.

Everything below is driven by one script, [`tools/restack.sh`](tools/restack.sh),
which you can run from a VSCode task without touching a terminal.

---

## 1. The model

Three kinds of objects. Confusing them is the only way to get into trouble.

| | what it is | do you edit it? |
|---|---|---|
| `upstream/dev` | the moving floor, from `meshcore-dev/MeshCore` | never |
| the 7 feature branches | **the source of truth** — one feature each | yes, this is where you work |
| `patch_public` | **a build artifact**, regenerated from the 7 | never directly |

`patch_public` is never edited and never merged into incrementally. It is thrown
away and rebuilt. That is what keeps its history readable, and what makes
"ship without feature X" a one-line change instead of an archaeology exercise.

> **There is no `dev` branch to update.** The local `dev` branch is a leftover,
> hundreds of commits behind, and nothing in this workflow reads it. The stack is
> built on `upstream/dev` — the *remote-tracking ref* — which `restack.sh`
> refreshes itself with `git fetch upstream`. You never check it out, never merge
> it, never pull it. Delete it if it bothers you.

### The stack

```
upstream/dev
 └─ feature/core-enhancements          shared hardening + the hooks the others need
     ├─ feature/repeater-ping           'ping' CLI command
     │   └─ feature/repeater-flood-retry   conditional flood retry
     ├─ feature/rp2040-lowpower         low-power idle, clock/voltage profiles
     │   └─ feature/lora-ota               mesh OTA + web updater
     ├─ tools/companion-tools           Python proxies + Telegram bridge
     └─ packaging/mlk-defaults          fork distribution defaults + this tooling
```

Most branches hang directly off `core-enhancements`. Two are stacked, and both
times for the same reason: the pair shares a single function, so no amount of
moving code around avoids a merge conflict — only stacking does.

- `flood-retry` on `ping`: both contribute to the body of `MyMesh::nextAppWake()`
  and both add state to `MyMesh`.
- `lora-ota` on `rp2040-lowpower`: the RP2040 clock / core-voltage profiles are
  power management, so `rp2040-lowpower` owns them; OTA only adds its own profile
  on top. This direction is deliberate — you can drop OTA and keep low power.

`packaging/mlk-defaults` is merged last so its distribution defaults win.

---

## 2. The three rules

Everything that has gone wrong with this stack traces back to breaking one of
these.

**Never `git pull` or `git merge upstream` into a stack branch.** Branches must
stay linear on top of their parent. A merge commit inside one desynchronises the
stack (that branch is up to date, the other six are not) and makes the next
rebase drop the merge and replay badly. Updating from upstream is
`./tools/restack.sh rebase`, applied to the *whole* stack, never to one branch.

**Never rebase a branch by hand before running the script.** `restack.sh`
computes every branch's old base from the state it finds. If you have already
moved one, its old tip is no longer an ancestor of its children, the computed
bases are wrong, and the parent's commits get replayed a second time on top of
themselves. It is all-or-nothing: let the script do all seven, or none.

**A clean merge proves nothing. Only a build does.** See §6.

---

## 3. From VSCode

Nothing to install. Git Bash ships with Git for Windows and both task runners
below pin it explicitly, so your terminal can stay on PowerShell. The script
re-execs itself from a copy outside the working tree, so it works from any
branch even though it only exists on `packaging/mlk-defaults`.

### PlatformIO — Project Tasks

The PlatformIO sidebar → **Project Tasks** → environment **`stack`** → group
**Custom**:

```
stack
└─ Custom
   ├─ 1. Check (read-only)
   ├─ 2. Rebase onto upstream
   ├─ 3. Regenerate patch_public
   ├─ 4. Build integration
   └─ Run tools unit tests
```

`stack` is a dummy environment declared at the top of the root `platformio.ini`.
It builds no firmware; PlatformIO has no project-level custom task list, so a
custom target has to belong to *some* environment. Being in the root file rather
than in a `variants/*/platformio.ini` puts it at the head of the list instead of
somewhere among the 500-odd board environments.

Equivalent from a terminal: `pio run -e stack -t stack-check` (or
`-t stack-rebase`, `-t stack-integrate`, `-t stack-build`, `-t stack-tests`).

The tests target deliberately runs under a Python *outside* PlatformIO's own
venv: the bot's dependencies belong in your environment, not in PlatformIO's.
If it reports a missing module it prints the `pip install` line to fix it.

### Plain VSCode tasks

Same commands without PlatformIO — `Ctrl+Shift+P` → **Tasks: Run Task** →

| task | what it does | destructive? |
|---|---|---|
| **Stack: check (read-only)** | how far behind upstream, and whether the 7 still merge | no |
| **Stack: rebase onto upstream** | restacks all seven onto their parents | **rewrites branches** |
| **Stack: regenerate patch_public** | rebuilds the integration branch (asks first) | **rewrites `patch_public`** |
| **Stack: build integration** | builds 5 environments | no |
| **Stack: update from upstream (full)** | rebase + regenerate + build | **both of the above** |
| **Stack: run tools unit tests** | the Python tests | no |

Prefer a terminal? `Ctrl+ù`, pick the *Git Bash* profile, then `./tools/restack.sh check`.
From PowerShell without switching shell:

```powershell
& "C:\Program Files\Git\bin\bash.exe" -c "./tools/restack.sh check"
```

---

## 4. Recipe: upstream has moved

This is the whole of it. There is no separate "update dev" step.

```bash
./tools/restack.sh check       # 1. read-only. Nothing is touched.
./tools/restack.sh rebase      # 2. rewrites all seven branches
./tools/restack.sh integrate   # 3. rebuilds patch_public (asks for confirmation)
```
then **Stack: build integration**, then push.

Roughly monthly, or when upstream lands something you want. The nightly CI
([`.github/workflows/stack-check.yml`](.github/workflows/stack-check.yml)) tells
you when you have drifted far enough to conflict.

### Example — a clean run

```
$ ./tools/restack.sh check

== behind upstream/dev
  feature/core-enhancements          9 commits
  feature/repeater-ping              9 commits
  ...
== merge dry-run onto a4ab7f0e
  feature/core-enhancements          ok
  ...
  clean - but rerere may have replayed a stale resolution. Build before trusting it.

$ ./tools/restack.sh rebase

== restacking onto a4ab7f0e
  feature/core-enhancements          onto upstream/dev
  feature/repeater-ping              onto feature/core-enhancements
  feature/repeater-flood-retry       onto feature/repeater-ping
  feature/rp2040-lowpower            onto feature/core-enhancements
  feature/lora-ota                   onto feature/rp2040-lowpower
  tools/companion-tools              onto feature/core-enhancements
  packaging/mlk-defaults             onto feature/core-enhancements
```

### Example — a conflict during `rebase`

```
  feature/core-enhancements          conflict - resolve, then: git rebase --continue

UU src/MeshCore.h
```

Upstream changed something your branch also touches. Fix it where it belongs —
in the branch:

```bash
code src/MeshCore.h              # resolve
git add src/MeshCore.h
git rebase --continue
./tools/restack.sh rebase        # re-run; branches already done are skipped
```

Re-running is safe: the script reports `already on <parent>` for anything whose
old base is already the parent's tip, and moves on.

### Example — a conflict during `integrate`

```
== merging 7 branches onto a4ab7f0e
  feature/core-enhancements          ok
  feature/repeater-ping              ok
  feature/repeater-flood-retry       conflict
    examples/simple_repeater/MyMesh.h
```

This means two of *your own* branches now contradict each other, which only
happens when upstream moved something both touch. `patch_public` was **not**
touched — you are on a scratch branch. Resolve it in the branch, not here,
otherwise you will resolve the same conflict at every regeneration:

```bash
git merge --abort
git checkout feature/repeater-flood-retry
# fix, commit
./tools/restack.sh rebase
./tools/restack.sh integrate
```

### Pushing

All seven branches were rewritten, so they need a force push. Use
`--force-with-lease`, never plain `--force`: it refuses if someone (or another
machine of yours) pushed in the meantime.

```bash
git push --force-with-lease origin \
  feature/core-enhancements feature/repeater-ping feature/repeater-flood-retry \
  feature/rp2040-lowpower feature/lora-ota tools/companion-tools \
  packaging/mlk-defaults patch_public
```

---

## 5. Recipe: you changed a feature

### A branch with no children

`repeater-flood-retry`, `lora-ota`, `companion-tools`, `packaging/mlk-defaults`.
Nothing depends on them, so there is nothing to restack.

```bash
git checkout feature/lora-ota
# ... edit, commit ...
./tools/restack.sh integrate
```
then build, then:
```bash
git push --force-with-lease origin feature/lora-ota patch_public
```

### A branch with children

`core-enhancements`, `repeater-ping`, `rp2040-lowpower`. Their children must be
replayed on the new tip.

```bash
git checkout feature/rp2040-lowpower
# ... edit, commit ...
./tools/restack.sh rebase       # replays lora-ota onto the new lowpower
./tools/restack.sh integrate
```
then build, then push the branch, its children, and `patch_public`.

> **Why `--onto`.** `STACK` in the script names each branch's *immediate* parent,
> and each branch is replayed with
> `git rebase --onto <parent> <old base> <branch>`, where every old base is
> captured **before anything moves**.
>
> The naive `git rebase <parent> <branch>` is wrong here. Once the parent has been
> rebased, its previous tip is no longer an ancestor of the child, so git falls
> back to a merge base further down — the old upstream — and replays *the
> parent's commits too*, on top of the already-rebased parent. The symptom is
> silent duplication: a declaration appearing two or three times, code that does
> not compile, and not a single reported conflict.

### Ship without a feature

Delete its line from `MERGE_ORDER` in [`tools/restack.sh`](tools/restack.sh),
then `integrate`. The branch still exists; it is simply no longer shipped.
Mind the stacking: dropping `rp2040-lowpower` also drops `lora-ota`, since OTA
sits on top of it. The reverse works — dropping only `lora-ota` is fine.

### Add a feature

```bash
git checkout -b feature/my-thing feature/core-enhancements
```
then add it in three places: `STACK` and `MERGE_ORDER` in the script, and the
`on: push: branches:` list in the workflow.

---

## 6. Three things that will bite you

**`rerere` replays, it does not validate.** Git records your conflict
resolutions and replays them automatically the next time the same conflict
appears — which is exactly what you want when re-merging the same branches over
and over. But it replays a *wrong* resolution just as happily as a right one.

This is not theoretical. It has happened twice on this stack: once reinstating a
resolution that had dropped a closing brace, once producing a `MeshCore.h` with
the same virtual declared three times. Both times the merge reported clean.

Worse, `rerere.autoupdate=true` makes it *stage* the replayed resolution, so a
rebase carries straight on without stopping. The script now forces
`-c rerere.autoupdate=false` on its own rebases and merges, but if you rebase by
hand, check your config:

```bash
git config --get rerere.autoupdate      # should be empty or false
rm -rf .git/rr-cache                    # when you suspect a stale resolution
```

**A clean `integrate` proves nothing. Only a build does.** That is why the build
appears in every recipe above, and why CI builds the merged result rather than
just checking that it merges.

**Branches that descend from `patch_public` break at every regeneration.**
`mqtt_espnow_multibridge` and `HTv4LowPower` were forked off it. Two options:
rebase them each time (tedious, forever), or re-parent them **once** onto what
they actually need — `upstream/dev`, or `feature/core-enhancements`. The second
is the right answer; they almost certainly do not need OTA or ping.

---

## 7. CI

[`.github/workflows/stack-check.yml`](.github/workflows/stack-check.yml) runs on
push to any of the 7 branches, **nightly**, and on demand. The nightly run is the
point: upstream drift breaks the stack silently, and a push trigger alone would
never notice.

| job | question it answers |
|---|---|
| `integrate` | do the 7 still merge onto current upstream, in order? |
| `build-integration` | does the merged result actually compile? |
| `build-branches` | does each branch still stand on its own? |
| `test-tools` | do the Python tests pass? |

`rerere` is deliberately **not** enabled in CI: it must see raw conflicts, not a
replay of a resolution recorded on someone's laptop.

CI never rebases and never pushes. A rebase needs judgement and a force push;
that stays local.

### Why `patch_public` is the fork's default branch

GitHub only honours `schedule:` for workflow files on the repository's **default
branch**. The workflow is authored on `packaging/mlk-defaults` and reaches
`patch_public` through the integration merge, so making `patch_public` the
default is what arms the nightly run. It also means a clone of the fork lands on
the firmware you actually ship, which is the point of the fork.

Two consequences of that choice, neither serious:

- The default branch gets **force-pushed** at every `integrate`. Anyone who
  cloned needs `git fetch && git reset --hard origin/patch_public` rather than a
  plain `git pull`. On a personal fork that is fine.
- Opening a PR **to upstream** pre-fills the head branch with the fork's default,
  i.e. `patch_public` — which is never what you want. Switch it to the feature
  branch in the PR form.

`github-pages.yml` is unaffected: it only triggers on push to `main`.

> GitHub disables scheduled workflows after 60 days without repository activity.
> If the fork goes quiet the nightly stops silently — re-arm it from the Actions
> tab with *Run workflow*.

---

## 8. The real goal

No tooling makes a 7-branch stack free to maintain. The only way to lower the
cost is to make the stack **shorter**.

`feature/core-enhancements`, `feature/repeater-ping` and
`feature/repeater-flood-retry` were written to be proposable upstream as they
stand. Every one that gets accepted is a branch you stop rebasing for life.
`feature/lora-ota` is bigger, but it is the one with the most value for the
upstream project.

`rp2040-lowpower`, `companion-tools` and `packaging/mlk-defaults` are legitimately
fork-specific and will stay.
