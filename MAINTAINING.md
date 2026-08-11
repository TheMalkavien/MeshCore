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

### The stack

```
upstream/dev
 └─ feature/core-enhancements          shared hardening + the hooks the others need
     ├─ feature/repeater-ping           'ping' CLI command
     │   └─ feature/repeater-flood-retry   conditional flood retry
     ├─ feature/rp2040-lowpower         low-power idle, clock/voltage profiles
     │   └─ feature/lora-ota               mesh OTA + web updater
     ├─ tools/companion-tools           Python proxies + Telegram bridge
     └─ packaging/mlk-defaults          fork distribution defaults (merged last)
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

## 2. From VSCode

Nothing to install. Git Bash ships with Git for Windows and the tasks pin it
explicitly, so your terminal can stay on PowerShell.

`Ctrl+Shift+P` → **Tasks: Run Task** →

| task | what it does | destructive? |
|---|---|---|
| **Stack: check (read-only)** | how far behind upstream, and whether the 7 still merge | no |
| **Stack: rebase onto upstream** | restacks every branch onto its parent | **rewrites branches** |
| **Stack: regenerate patch_public** | rebuilds the integration branch (asks first) | **rewrites `patch_public`** |
| **Stack: build integration** | builds 5 environments | no |
| **Stack: update from upstream (full)** | rebase + regenerate + build | **both of the above** |
| **Stack: run tools unit tests** | the 11 Python tests | no |

Prefer a terminal? `Ctrl+ù`, pick the *Git Bash* profile, then `./tools/restack.sh check`.
From PowerShell without switching shell:

```powershell
& "C:\Program Files\Git\bin\bash.exe" -c "./tools/restack.sh check"
```

---

## 3. Recipe: upstream has moved

Roughly monthly, or when upstream lands something you want. The nightly CI
([`.github/workflows/stack-check.yml`](.github/workflows/stack-check.yml)) tells
you when you have drifted far enough to conflict.

```bash
./tools/restack.sh check       # 1. read-only. Nothing is touched.
./tools/restack.sh rebase      # 2. rewrites the 7 branches
./tools/restack.sh integrate   # 3. rebuilds patch_public (asks for confirmation)
```
then **Stack: build integration**, then push.

### Example — a clean run

```
$ ./tools/restack.sh check

== behind upstream/dev
  feature/core-enhancements          12 commits
  feature/repeater-ping              12 commits
  ...
== merge dry-run onto 9f2ab110
  feature/core-enhancements          ok
  feature/repeater-ping              ok
  ...
  clean - but rerere may have replayed a stale resolution. Build before trusting it.
```

### Example — a conflict during `rebase`

```
$ ./tools/restack.sh rebase

== restacking onto 9f2ab110
  feature/core-enhancements          onto upstream/dev
  feature/repeater-ping              conflict - resolve, then: git rebase --continue

UU examples/simple_repeater/MyMesh.cpp
```

Upstream changed something your branch also touches. Fix it where it belongs —
in the branch:

```bash
code examples/simple_repeater/MyMesh.cpp   # resolve
git add examples/simple_repeater/MyMesh.cpp
git rebase --continue
./tools/restack.sh rebase                  # re-run; branches already done are no-ops
```

### Example — a conflict during `integrate`

```
$ ./tools/restack.sh integrate

== merging 7 branches onto 9f2ab110
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

The 7 branches were rewritten, so they need a force push. Use
`--force-with-lease`, never plain `--force`: it refuses if someone (or another
machine of yours) pushed in the meantime.

```bash
git push --force-with-lease origin \
  feature/core-enhancements feature/repeater-ping feature/repeater-flood-retry \
  feature/rp2040-lowpower feature/lora-ota tools/companion-tools \
  packaging/mlk-defaults patch_public
```

---

## 4. Recipe: you changed a feature

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
then build, then push both the branch and its children.

> This is why `STACK` in the script names each branch's **immediate** parent
> rather than the root of its chain. `git rebase <parent> <branch>` then replays
> only the commits the branch owns, because the merge base is the parent's
> previous tip. Naming the root instead would replay the whole chain and
> silently drop the commit you just made to the link above.

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

## 5. Two things that will bite you

**`rerere` replays, it does not validate.** Git records your conflict
resolutions and replays them automatically the next time the same conflict
appears — which is exactly what you want when re-merging the same branches over
and over. But it replays a *wrong* resolution just as happily as a right one.
During the initial split it silently reinstated a resolution that had dropped a
closing brace, and the merge still reported clean. **A clean `integrate` proves
nothing. Only a build does.** If you ever suspect a stale resolution:

```bash
rm -rf .git/rr-cache
```

**Branches that descend from `patch_public` break at every regeneration.**
`mqtt_espnow_multibridge` and `HTv4LowPower` were forked off it. Two options:
rebase them each time (tedious, forever), or re-parent them **once** onto what
they actually need — `upstream/dev`, or `feature/core-enhancements`. The second
is the right answer; they almost certainly do not need OTA or ping.

---

## 6. CI

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

> GitHub disables scheduled workflows on a repository after 60 days without
> activity. If the fork goes quiet, the nightly run stops silently — re-arm it
> from the Actions tab with *Run workflow*.

---

## 7. The real goal

No tooling makes a 7-branch stack free to maintain. The only way to lower the
cost is to make the stack **shorter**.

`feature/core-enhancements`, `feature/repeater-ping` and
`feature/repeater-flood-retry` were written to be proposable upstream as they
stand. Every one that gets accepted is a branch you stop rebasing for life.
`feature/lora-ota` is bigger, but it is the one with the most value for the
upstream project.

`rp2040-lowpower`, `companion-tools` and `packaging/mlk-defaults` are legitimately
fork-specific and will stay.
