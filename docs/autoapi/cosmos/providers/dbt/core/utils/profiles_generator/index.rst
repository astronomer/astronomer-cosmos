:py:mod:`cosmos.providers.dbt.core.utils.profiles_generator`
============================================================

.. py:module:: cosmos.providers.dbt.core.utils.profiles_generator


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.utils.profiles_generator.create_default_profiles
   cosmos.providers.dbt.core.utils.profiles_generator.map_profile
   cosmos.providers.dbt.core.utils.profiles_generator.conn_exists



Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.utils.profiles_generator.logger


.. py:data:: logger



.. py:function:: create_default_profiles(profile_path: pathlib.Path) -> None

   Write all the available profiles out to the profile path.
   :param profile_path: The path location to write all the profiles to.
   :return: Nothing


.. py:function:: map_profile(conn_id: str, db_override: Optional[str] = None, schema_override: Optional[str] = None) -> Tuple[str, dict]


.. py:function:: conn_exists(conn_id: str) -> bool
