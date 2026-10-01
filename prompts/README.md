# The prompts

These are the exact prompts used to build NewsWave with Claude Code (Opus 5.5 as orchestrator, Sonnet 5.5 sub-agents). Nothing was edited, except that the surrounding paste markers were removed.

1. [`01-original-prompt.txt`](01-original-prompt.txt) - the full spec, first drafted with ChatGPT. The last line tells Claude Code to use Sonnet 5.5 sub-agents with clear end states and to act as the orchestrator.
2. [`02-follow-up.txt`](02-follow-up.txt) - sent after the first prompt: use the Claude CLI login, not the API.

`docs/SPEC.md` is a cleaned, reformatted copy of prompt 1 with this follow-up applied as an amendment. `docs/CONTRACT.md` records the decisions the agents made.
