## Pull Request Template for No Framework Pickler

---

**(Optional) Deep Research Review**: When submitting a PR, please consider using a "deep research" tool to sanity check your proposal. Then **before** submission, run your draft through a strong model with a prompt such as:

> "Please review the AGENTS.md and README.md along with this draft PR and check that it does not have any gaps — why it might be insufficient, incomplete, lacking a concrete example, duplicating prior issues or PRs, or not aligned with the project goals or non‑goals for a Java 21+ Data-Oriented Programming serialization library."

**(Optional)** Please then attach both the prompt and the model's review to the bottom of this template under "Augmented Intelligence Review".

---

## What changed

- A summary of the changes in this PR

## Why this change is needed

- Motivation / context
- Does this address a specific issue with serialization performance, type safety, or schema evolution?

## How were these changes tested

- Test plan / manual testing / automated tests
- For serialization changes: Include round-trip tests with various data types
- For performance changes: Include before/after measurements if applicable
- Use `./mvn-test-no-boilerplate.sh -Dtest=YourTests -Djava.util.logging.ConsoleHandler.level=FINE` for focused testing

## Schema Evolution Impact

- [ ] No impact on existing serialized data
- [ ] Requires migration (if so, provide migration guide)
- [ ] Changes are backwards compatible with `ENABLED` compatibility mode
- [ ] New type signatures documented if applicable

## Type Safety Verification

- [ ] All new code uses exhaustive pattern matching where appropriate
- [ ] No magic numbers - constants defined using enums
- [ ] AST grammar impact considered (see ARCHITECTURE.md)

## Checklist

- [ ] Code builds / passes tests (requires Java 21+)
- [ ] New tests added using TDD approach (Red-Green-Refactor)
- [ ] Follows Data-Oriented Programming principles from `CODING_STYLE_LLM.md`
- [ ] Uses JEP 467 markdown documentation (`/// comments`) not legacy JavaDoc
- [ ] Documentation updated if needed (README.md, ARCHITECTURE.md, BACKWARDS_COMPATIBILITY.md)
- [ ] `AGENTS.md` updated if appropriate for AI agent guidance
- [ ] Performance impact on serialize/deserialize hot paths considered
- [ ] Zero external dependencies maintained

## Performance Notes

*(If applicable)*
- Hot path performance impact: 
- Memory allocation impact:
- ByteBuffer efficiency considerations:

**(Optional) Augmented Intelligence Review**: 
*Both prompt and model output, asking a strong model to double-check your submission from the perspective of a maintainer of this Java serialization library*