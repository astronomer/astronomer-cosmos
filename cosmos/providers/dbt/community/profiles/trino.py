from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import Connection

AVAILABLE_AUTH_TYPES = ("certs", "kerberos", "jwt", "ldap")

# Optional environment variables have to have a default value with the same data type that an expected value would have.
trino_profile = {
    "outputs": {
        "dev": {
            "type": "trino",
            "method": "{{ env_var('TRINO_AUTH_TYPE') }}",
            "http_scheme": "{{ env_var('TRINO_HTTP_SCHEME', 'http') }}",
            "database": "{{ env_var('TRINO_DATABASE')}} ",
            "schema": "{{ env_var('TRINO_SCHEMA')}} ",
            "host": "{{ env_var('TRINO_HOST') }}",
            "port": "{{ env_var('TRINO_PORT') | as_number }}",
            "session_properties": "{{ env_var('TRINO_SESSION_PROPERTIES', {}) }}",
            # Ldap auth and others
            "user": "{{ env_var('TRINO_USER', '') }}",
            "password": "{{ env_var('TRINO_PASSWORD', '') }}",
            # Certificate auth
            "client_certificate": "{{ env_var('TRINO_CLIENT_CERTIFICATE', '') }}",
            "client_private_key": "{{ env_var('TRINO_CLIENT_PRIVATE_KEY', '') }}",
            # JWT token auth
            "jwt_token": "{{ env_var('TRINO_JWT_TOKEN', '') }}",
            # Kerberos auth
            "krb5_config": "{{ env_var('TRINO_KRB5_CONFIG', '') }}",
            "principal": "{{ env_var('TRINO_PRINCIPAL', '') }}",
            "service_name": "{{ env_var('TRINO_SERVICE_NAME', '') }}",
            "mutual_authentication": "{{ env_var('TRINO_MUTUAL_AUTHENTICATION', '') }}",
            "force_preemptive": "{{ env_var('TRINO_FORCE_PREEMPTIVE', '') }}",
            "hostname_override": "{{ env_var('TRINO_HOSTNAME_OVERRIDE', '') }}",
            "sanitize_mutual_error_response": "{{ env_var('TRINO_SANITIZE_MUTUAL_ERROR_RESPONSE', '') }}",
            "delegate": "{{ env_var('TRINO_DELEGATE', '') }}",
            "cert": "{{ env_var('TRINO_CA_BUNDLE', '') }}",
        }
    },
    "target": "dev",
}


def create_profile_vars_kerberos(
    conn: Connection,
    common_profile_vars: dict[str, str],
) -> dict[str, str]:
    """
    Key tab is not supported by the Trino Airflow connection, in dbt it's set as an env var.
        https://github.com/starburstdata/dbt-trino/blob/v1.4.0/dbt/adapters/trino/connections.py#L204
    A key tab is used in place of a password, so we'll have to enforce that a password is present.

    Airflow: kerberos__config                                dbt: krb5_config
    Airflow: kerberos__principal                             dbt: principal
    Airflow: kerberos__service_name                          dbt: service_name
    Airflow: kerberos__mutual_authentication | False         dbt: mutual_authentication
    Airflow: kerberos__force_preemptive | False              dbt: force_preemptive
    Airflow: kerberos__hostname_override                     dbt: hostname_override
    Airflow: kerberos__sanitize_mutual_error_response | True dbt: sanitize_mutual_error_response
    Airflow: kerberos__delegate | False                      dbt: delegate
    Airflow: kerberos__ca_bundle                             dbt: cert
    """
    common_profile_vars["TRINO_AUTH_TYPE"] = "kerberos"
    extra_dejson = conn.extra_dejson
    # Mandatory fields
    krb5_config = str(extra_dejson.get("kerberos__config"))
    principal = str(extra_dejson.get("kerberos__principal"))
    password = str(conn.password)
    if not all((Path(krb5_config).exists(), principal, password)):
        raise ValueError("One of kerberos__config, kerberos__principal or password is missing/incorrect")
    common_profile_vars["TRINO_PASSWORD"] = password
    common_profile_vars["TRINO_KRB5_CONFIG"] = krb5_config
    common_profile_vars["TRINO_PRINCIPAL"] = principal
    # Optional fields
    common_profile_vars["TRINO_SERVICE_NAME"] = str(extra_dejson.get("kerberos__service_name"))
    common_profile_vars["TRINO_MUTUAL_AUTHENTICATION"] = str(extra_dejson.get("kerberos__mutual_authentication"))
    common_profile_vars["TRINO_FORCE_PREEMPTIVE"] = str(extra_dejson.get("kerberos__force_preemptive"))
    common_profile_vars["TRINO_HOSTNAME_OVERRIDE"] = str(extra_dejson.get("kerberos__hostname_override"))
    common_profile_vars["TRINO_SANITIZE_MUTUAL_ERROR_RESPONSE"] = str(
        extra_dejson.get("kerberos__sanitize_mutual_error_response")
    )
    common_profile_vars["TRINO_DELEGATE"] = str(extra_dejson.get("kerberos__delegate"))
    # Not sure if this is supposed to be a file or not.
    # https://github.com/trinodb/trino-python-client/blob/0.321.0/trino/auth.py#L84
    common_profile_vars["TRINO_CA_BUNDLE"] = str(extra_dejson.get("kerberos__ca_bundle"))

    return common_profile_vars


def create_profile_vars_ldap(
    conn: Connection,
    common_profile_vars: dict[str, str],
) -> dict[str, str]:
    """
    All we need here is a user and password.
    """
    common_profile_vars["TRINO_AUTH_TYPE"] = "ldap"
    # Validate mandatory fields
    if not all((conn.login, conn.password)):
        raise ValueError("Trino auth type: ldap, missing login or password")
    common_profile_vars["TRINO_USER"] = conn.login
    common_profile_vars["TRINO_PASSWORD"] = conn.password

    return common_profile_vars


def create_profile_vars_certs(conn: Connection, common_profile_vars: dict[str, str]) -> dict[str, str]:
    """
    Airflow: certs__client_cert_path = dbt: client_certificate
    Airflow: certs__client_key_path  = dbt: client_private_key
    """
    common_profile_vars["TRINO_AUTH_TYPE"] = "certificate"
    extra_dejson = conn.extra_dejson
    cert_path = Path(str(extra_dejson.get("certs__client_cert_path")))
    key_path = Path(str(extra_dejson.get("certs__client_key_path")))
    if not cert_path.exists():
        raise ValueError(f"certs__client_cert_path: {cert_path} does not exist")
    if not key_path.exists():
        raise ValueError(f"certs__client_key_path: {key_path} does not exist")
    common_profile_vars["TRINO_CLIENT_CERTIFICATE"] = str(cert_path)
    common_profile_vars["TRINO_CLIENT_PRIVATE_KEY"] = str(key_path)

    return common_profile_vars


def create_profile_vars_jwt(conn: Connection, common_profile_vars: dict[str, str]) -> dict[str, str]:
    """
    Airflow: jwt__token = dbt: jwt_token
    """
    common_profile_vars["TRINO_AUTH_TYPE"] = "jwt"
    extra_dejson = conn.extra_dejson
    jwt_token = extra_dejson.get("jwt__token")
    if not jwt_token:
        raise ValueError("jwt__token needed for jwt based authentication")
    common_profile_vars["jwt_token"] = jwt_token
    return common_profile_vars


def create_profile_vars_trino(
    conn: Connection,
    database_override: str | None = None,
    schema_override: str | None = None,
) -> tuple[str, dict[str, str]]:
    """
    https://docs.getdbt.com/reference/warehouse-setups/trino-setup
    https://airflow.apache.org/docs/apache-airflow-providers-trino/stable/connections.html

    Airflow supports ldap, kerberos, jwt token and certificates.
    dbt additionally supports oauth and none which we will not support here.
    """
    extra_dejson = conn.extra_dejson
    auth_type = extra_dejson.get("auth", "ldap")
    if auth_type not in AVAILABLE_AUTH_TYPES:
        raise ValueError(
            f"Trino auth type: {auth_type} is not allowed, choose one of {AVAILABLE_AUTH_TYPES} or leave blank for ldap"
        )
    common_vars = {
        "TRINO_HTTP_SCHEME": str(extra_dejson.get("protocol")),
        "TRINO_DATABASE": database_override if database_override is not None else str(extra_dejson.get("catalog")),
        "TRINO_SCHEMA": schema_override if schema_override is not None else conn.schema,
        "TRINO_HOST": conn.host,
        "TRINO_PORT": str(conn.port),
    }

    if extra_dejson.get("session_properties"):
        common_vars["TRINO_SESSION_PROPERTIES"] = str(extra_dejson.get("session_properties"))

    dispatch = {
        "ldap": create_profile_vars_ldap,
        "kerberos": create_profile_vars_kerberos,
        "certs": create_profile_vars_certs,
        "jwt": create_profile_vars_jwt,
    }

    return "trino_profile", dispatch[auth_type](conn, common_vars)
