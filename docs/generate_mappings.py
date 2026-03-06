"""
Script to generate a dedicated docs page per profile mapping.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from jinja2 import Environment, FileSystemLoader

from cosmos.profiles import BaseProfileMapping, profile_mappings


@dataclass
class Field:
    """Represents a field in a profile mapping."""

    dbt_name: str
    required: bool = False
    airflow_name: str | list[str] | None = None


def get_fields_from_mapping(mapping: type[BaseProfileMapping]) -> list[Field]:
    """
    Generates Field objects from a profile mapping.
    """
    fields = []
    required_fields = mapping.required_fields

    # get the fields from the airflow param mapping
    for key, val in mapping.airflow_param_mapping.items():
        is_required = key in required_fields
        fields.append(Field(dbt_name=key, required=is_required, airflow_name=val))

    # add the required fields that are not in the airflow param mapping
    for field in required_fields:
        if field not in mapping.airflow_param_mapping:
            fields.append(Field(dbt_name=field, required=True))

    return fields


def generate_mapping_docs(
    templates_dir: str = "./templates",
    output_dir: str = "./profiles",
) -> None:
    """
    Generate a dedicated docs page per profile mapping.
    """
    # first, remove the existing docs
    if os.path.exists(output_dir):
        for file in os.listdir(output_dir):
            os.remove(f"{output_dir}/{file}")

    # then, create the directory
    os.makedirs(output_dir, exist_ok=True)

    # get the index template
    env = Environment(loader=FileSystemLoader(templates_dir))
    index_template = env.get_template("index.rst.jinja2")

    mapping_template = env.get_template("profile_mapping.rst.jinja2")
    # generate the profile mapping pages
    for mapping in profile_mappings:
        with open(f"{output_dir}/{mapping.__name__.replace('ProfileMapping', '')}.rst", "w", encoding="utf-8") as f:
            docstring = mapping.__doc__ or ""
            f.write(
                mapping_template.render(
                    {
                        "mapping_name": mapping.__name__.replace("ProfileMapping", ""),
                        "mapping_description": "\n\n".join(docstring.split("\n")),
                        "fields": [field.__dict__ for field in get_fields_from_mapping(mapping=mapping)],
                        "airflow_conn_type": mapping.airflow_connection_type,
                        "profile_defaults": getattr(mapping, "profile_defaults", {}),
                    }
                )
            )

    # generate the index page
    with open(f"{output_dir}/index.rst", "w", encoding="utf-8") as f:
        f.write(
            index_template.render(
                profile_mapping_names=[mapping.__name__.replace("ProfileMapping", "") for mapping in profile_mappings]
            )
        )
