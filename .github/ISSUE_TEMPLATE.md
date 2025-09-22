# Issue Template

(Optional) When submitting an Issue, please consider using a "deep research" tool to sanity check your proposal. Then **before** submission, run your draft through a strong model with a prompt such as:

> "Please review the AGENTS.md and README.md along with this draft Issue and check that it does not have any gaps — why it might be insufficient, incomplete, lacking a concrete example, duplicating prior issues or PRs, or not be aligned with the project goals or non‑goals."

(Optional) Please then attach both the prompt and the model's review to the bottom of this template under "Augmented Intelligence Review".

---

## Describe the Issue or Feature Request

**Title**:  
*A short, descriptive title*

**What happened / What you expected**:  
*Please provide plenty of text describing the issue or feature request. For bugs, explain what you expected to happen vs. what actually happened. For features, explain the problem this would solve and how it fits with the project's Data-Oriented Programming principles.*

**Steps to reproduce** (for bugs):

1. Create a record/sealed interface with...
2. Use `Pickler.forClass()` to...
3. Call `serialize()` or `deserialize()` with...
4. Observe the error/unexpected behavior

**Environment** (for bugs):

- Java version:
- Operating System:
- No Framework Pickler version/commit:

**Suggested solution / feature description**:  
*Please ensure that suggestions align with the [Project Goals](../README.md#project-goals) stated in the README:*

- *Must support Java 21+ features (records, sealed interfaces, pattern matching)*
- *Must maintain zero dependencies*
- *Must use Data-Oriented Programming principles*
- *Must follow TDD methodology*
- *Must preserve type safety and performance characteristics*

**Additional context / logs / screenshots**:  
*Screenshots and traces last. Please ensure the details above work for anyone, and use this section for your specifics. Include relevant stack traces, logging output (consider using `-Djava.util.logging.ConsoleHandler.level=FINER`), or code examples.*

**Checklist**:

- [ ] I have searched existing issues to avoid duplicates
- [ ] This aligns with the [Project Goals](../README.md#project-goals)
- [ ] I have provided sufficient context for reproduction (for bugs)
- [ ] I have considered the performance implications (Meta-stage vs Object-stage)
- [ ] I understand this is a zero-dependency, annotation-free library

## **(Optional) Augmented Intelligence Review**

Both prompt and model output, asking a strong model to double-check your submission, from the perspective of a maintainer of this repo
