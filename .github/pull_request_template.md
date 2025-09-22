# Pull Request Template

(Optional) When submitting a PR, please consider using a "deep research" tool to sanity check your proposal. Then **before** submission, run your draft through a strong model with a prompt such as:

> "Please review the AGENTS.md and README.md along with this draft PR and check that it does not have any gaps — why it might be insufficient, incomplete, lacking a concrete example, duplicating prior issues or PRs, or not be aligned with the project goals or non‑goals."

(Optional) Please then attach both the prompt and the model's review to the bottom of this template under "Augmented Intelligence Review".

---

## What changed

- A summary of the changes in this PR

## Why this change is needed

- Motivation / context

## How were these changes tested

- Test plan / manual testing / automated tests etc.

## Checklist

- [ ] Code builds / passes tests  
- [ ] New tests added if needed
- [ ] Updated to use `CODING_STYLE_LLM.md` conventions
- [ ] Documentation updated if needed  
- [ ] `AGENTS.md` updated if appropriate
- [ ] Follows TDD methodology (Red-Green-Refactor)
- [ ] Uses JEP 467 Markdown documentation comments (`///`) not legacy JavaDoc
- [ ] Package-private scope used by default, public only for cross-package APIs
- [ ] No magic numbers - uses enum constants
- [ ] Uses functional style with Stream API where appropriate

## **(Optional) Augmented Intelligence Review**

Both prompt and model output, asking a strong model to double-check your submission, from the perspective of a maintainer of this repo
