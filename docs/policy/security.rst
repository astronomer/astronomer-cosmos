Cosmos security
---------------

Security policy
+++++++++++++++

Check the project's `Security Policy <https://github.com/astronomer/astronomer-cosmos/blob/main/SECURITY.rst>`_ to learn
how to report security vulnerabilities in Astronomer Cosmos and how security issues reported to the Astronomer Cosmos
security team are handled.

Dependency update cooldown
++++++++++++++++++++++++++

To mitigate the risk of supply chain attacks from newly released versions, Cosmos enforces a **7-day cooldown period**
before merging automated dependency update pull requests. This cooldown gives time for the broader community to discover
and report potential supply chain compromises in new releases before they are adopted into the project.

This policy currently applies to:

- GitHub Actions version updates
- Pre-commit hook updates

These updates are managed via `Dependabot <https://docs.github.com/en/code-security/dependabot/dependabot-version-updates>`_, which is
configured with a 7-day cooldown setting for these updates.
