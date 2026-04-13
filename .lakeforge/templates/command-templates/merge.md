---
description: Merge a completed feature into the main branch and clean up worktree
scripts:
  sh: "lakeforge agent feature merge"
  ps: "lakeforge agent"
---
**Path reference rule:** When you mention directories or files, provide either the absolute path or a path relative to the project root (for example, `lakeforge-specs/<feature>/tasks/`). Never refer to a folder by name alone.

*Path: [templates/commands/merge.md](templates/commands/merge.md)*


# Merge Feature Branch

This command merges a completed feature branch into the main/target branch and handles cleanup of worktrees and branches.

## ⛔ Location Pre-flight Check (CRITICAL)

**BEFORE PROCEEDING:** You MUST be in the feature worktree, NOT the main repository.

Verify your current location:
```bash
pwd
git branch --show-current
```

**Expected output:**
- `pwd`: Should end with `.worktrees/001-feature-name` (or similar feature worktree)
- Branch: Should show your feature branch name like `001-feature-name` (NOT `main` or `release/*`)

**If you see:**
- Branch showing `main` or `release/`
- OR pwd shows the main repository root

⛔ **STOP - DANGER! You are in the wrong location!**

This command merges your feature INTO main. Running from the wrong location can cause:
- Loss of work
- Merge conflicts
- Repository corruption

**Correct the issue:**
1. Navigate to your feature worktree: `cd .worktrees/001-feature-name`
2. Verify you're on the correct feature branch: `git branch --show-current`
3. Then run this merge command again

**Exception (main branch):**
If you are on `main` and need to merge a workspace-per-WP feature, run:
```bash
lakeforge merge --feature <feature-slug>
```

---

## Prerequisites

Before running this command:

1. ✅ Feature must pass `/accept` checks
2. ✅ All work packages must be in `tasks/`
3. ✅ Working directory must be clean (no uncommitted changes)
4. ✅ Run the command from the feature worktree (Lakeforge will move the merge to the primary repo automatically)

## Location Pre-flight Check (CRITICAL for AI Agents)

Before merging, verify you are in the correct working directory by running this validation:

```bash
python3 -c "
from lakeforge_cli.guards import validate_worktree_location
result = validate_worktree_location()
if not result.is_valid:
    print(result.format_error())
    print('\nThis command MUST run from a feature worktree, not the main repository.')
    print('\nFor workspace-per-WP features, run from ANY WP worktree:')
    print('  cd /path/to/project/.worktrees/<feature>-WP01')
    print('  # or any other WP worktree for this feature')
    raise SystemExit(1)
else:
    print('✓ Location verified:', result.branch_name)
"
```

**What this validates**:
- Current branch follows the feature pattern like `001-feature-name` or `001-feature-name-WP01`
- You're not attempting to run from `main` or any release branch
- The validator prints clear navigation instructions if you're outside the feature worktree

**For workspace-per-WP features (0.11.0+)**:
- Run merge from ANY WP worktree (e.g., `.worktrees/014-feature-WP09/`)
- The merge command automatically detects all WP branches and merges them sequentially
- You do NOT need to run merge from each WP worktree individually

## What This Command Does

1. **Detects** your current feature branch and worktree status
2. **Runs** pre-flight validation across all worktrees and the target branch
3. **Determines** merge order based on WP dependencies (workspace-per-WP)
4. **Forecasts** conflicts during `--dry-run` and flags auto-resolvable status files
5. **Verifies** working directory is clean (legacy single-worktree)
6. **Switches** to the target branch (default: `main`) in the primary repository
7. **Updates** the target branch (`git pull --ff-only`)
8. **Merges** the feature using your chosen strategy
9. **Auto-resolves** status file conflicts after each WP merge
10. **Optionally pushes** to origin
11. **Removes** the feature worktree (if in one)
12. **Deletes** the feature branch

## Usage

### Basic merge (default: merge commit, cleanup everything)

```bash
lakeforge merge
```

This will:
- Create a merge commit
- Remove the worktree
- Delete the feature branch
- Keep changes local (no push)

### Merge with options

```bash
# Squash all commits into one
lakeforge merge --strategy squash

# Push to origin after merging
lakeforge merge --push

# Keep the feature branch
lakeforge merge --keep-branch

# Keep the worktree
lakeforge merge --keep-worktree

# Merge into a different branch
lakeforge merge --target develop

# See what would happen without doing it
lakeforge merge --dry-run

# Run merge from main for a workspace-per-WP feature
lakeforge merge --feature 017-feature-slug
```

### Common workflows

```bash
# Feature complete, squash and push
lakeforge merge --strategy squash --push

# Keep branch for reference
lakeforge merge --keep-branch

# Merge into develop instead of main
lakeforge merge --target develop --push
```

## Merge Strategies

### `merge` (default)
Creates a merge commit preserving all feature branch commits.
```bash
lakeforge merge --strategy merge
```
✅ Preserves full commit history
✅ Clear feature boundaries in git log
❌ More commits in main branch

### `squash`
Squashes all feature commits into a single commit.
```bash
lakeforge merge --strategy squash
```
✅ Clean, linear history on main
✅ Single commit per feature
❌ Loses individual commit details

### `rebase`
Requires manual rebase first (command will guide you).
```bash
lakeforge merge --strategy rebase
```
✅ Linear history without merge commits
❌ Requires manual intervention
❌ Rewrites commit history

## Options

| Option | Description | Default |
|--------|-------------|---------|
| `--strategy` | Merge strategy: `merge`, `squash`, or `rebase` | `merge` |
| `--delete-branch` / `--keep-branch` | Delete feature branch after merge | delete |
| `--remove-worktree` / `--keep-worktree` | Remove feature worktree after merge | remove |
| `--push` | Push to origin after merge | no push |
| `--target` | Target branch to merge into | `main` |
| `--dry-run` | Show what would be done without executing | off |
| `--feature` | Feature slug when merging from main branch | none |
| `--resume` | Resume an interrupted merge | off |

## Worktree Strategy

Lakeforge uses an **opinionated worktree approach**:

### Workspace-per-WP Model (0.11.0+)

In the current model, each work package gets its own worktree:

```
my-project/                              # Main repo (main branch)
├── .worktrees/
│   ├── 001-auth-system-WP01/           # WP01 worktree
│   ├── 001-auth-system-WP02/           # WP02 worktree
│   ├── 001-auth-system-WP03/           # WP03 worktree
│   └── 002-dashboard-WP01/             # Different feature
├── .lakeforge/
├── lakeforge-specs/
└── ... (main branch files)
```

**Merge behavior for workspace-per-WP**:
- Run `lakeforge merge` from **any** WP worktree for the feature
- The command automatically detects all WP branches (WP01, WP02, WP03, etc.)
- Merges each WP branch into main in sequence
- Cleans up all WP worktrees and branches

### Legacy Pattern (0.10.x)
```
my-project/                    # Main repo (main branch)
├── .worktrees/
│   ├── 001-auth-system/      # Feature 1 worktree (single)
│   ├── 002-dashboard/        # Feature 2 worktree (single)
│   └── 003-notifications/    # Feature 3 worktree (single)
├── .lakeforge/
├── lakeforge-specs/
└── ... (main branch files)
```

### The Rules
1. **Main branch** stays in the primary repo root
2. **Feature branches** live in `.worktrees/<feature-slug>/`
3. **Work on features** happens in their worktrees (isolation)
4. **Merge from worktrees** using this command – the CLI will hop to the primary repo for the Git merge
5. **Cleanup is automatic** - worktrees removed after merge

### Why Worktrees?
- ✅ Work on multiple features simultaneously
- ✅ Each feature has its own sandbox
- ✅ No branch switching in main repo
- ✅ Easy to compare features
- ✅ Clean separation of concerns

### The Flow
```
1. /specify           → Creates branch + worktree
2. cd .worktrees/<feature>/      → Enter worktree
3. /plan              → Work in isolation
4. /tasks
5. /implement
6. /review
7. /accept
8. /merge             → Merge + cleanup worktree
9. Back in main repo!            → Ready for next feature
```

## Error Handling

### "Already on main branch"
You're not on a feature branch. Switch to your feature branch first:
```bash
cd .worktrees/<feature-slug>
# or
git checkout <feature-branch>
```

### "Working directory has uncommitted changes"
Commit or stash your changes:
```bash
git add .
git commit -m "Final changes"
# or
git stash
```

### "Could not fast-forward main"
Your main branch is behind origin:
```bash
git checkout main
git pull
git checkout <feature-branch>
lakeforge merge
```

### "Merge failed - conflicts"
Resolve conflicts manually:
```bash
# Fix conflicts in files
git add <resolved-files>
git commit
# Then complete cleanup manually:
git worktree remove .worktrees/<feature>
git branch -d <feature-branch>
```

## Safety Features

1. **Clean working directory check** - Won't merge with uncommitted changes
2. **Primary repo hand-off** - Automatically runs Git operations from the main checkout when invoked in a worktree
3. **Fast-forward only pull** - Won't proceed if main has diverged
4. **Graceful failure** - If merge fails, you can fix manually
5. **Optional operations** - Push, branch delete, and worktree removal are configurable
6. **Dry run mode** - Preview exactly what will happen

## Examples

### Complete feature and push
```bash
cd .worktrees/001-auth-system
/accept
/merge --push
```

### Squash merge for cleaner history
```bash
lakeforge merge --strategy squash --push
```

### Merge but keep branch for reference
```bash
lakeforge merge --keep-branch --push
```

### Check what will happen first
```bash
lakeforge merge --dry-run
```

## After Merging

After a successful merge, you're back on the main branch with:
- ✅ Feature code integrated
- ✅ Worktree removed (if it existed)
- ✅ Feature branch deleted (unless `--keep-branch`)
- ✅ Ready to start your next feature!

## Integration with Accept

The typical flow is:

```bash
# 1. Run acceptance checks
/accept --mode local

# 2. If checks pass, merge
/merge --push
```

Or combine conceptually:
```bash
# Accept verifies readiness
/accept --mode local

# Merge performs integration
/merge --strategy squash --push
```

The `/accept` command **verifies** your feature is complete.
The `/merge` command **integrates** your feature into main.

Together they complete the workflow:
```
specify → plan → tasks → implement → review → accept → merge ✅
```
