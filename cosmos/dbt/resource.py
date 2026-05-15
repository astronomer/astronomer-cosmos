from __future__ import annotations


def get_resource_name_from_unique_id(unique_id: str) -> str:
    """
    Return the ``resource_name`` segment of a dbt node ``unique_id``.

    Per the `dbt manifest spec
    <https://docs.getdbt.com/reference/artifacts/manifest-json#resource-details>`_,
    a node ``unique_id`` is ``<resource_type>.<package>.<resource_name>``.
    Both ``resource_type`` and ``package`` are constrained identifiers that
    cannot contain dots, so the first two dots are unambiguous separators
    and everything after the second dot is the full resource name.

    For versioned models, dbt appends a fourth segment:
    ``model.<package>.<resource_name>.<version>`` (see
    `node_args.py <https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/contracts/graph/node_args.py#L26C3-L31>`_).
    Splitting with ``maxsplit=2`` preserves that suffix:
    ``model.pkg.my_model.v1`` -> ``my_model.v1``.

    :raises IndexError: if ``unique_id`` does not contain at least two
        dots. Malformed inputs are surfaced loudly rather than silently
        mis-parsed.
    """
    return unique_id.split(".", 2)[2]
