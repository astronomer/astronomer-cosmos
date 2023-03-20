:py:mod:`cosmos.providers.dbt.parser.project`
=============================================

.. py:module:: cosmos.providers.dbt.parser.project

.. autoapi-nested-parse::

   Used to parse and extract information from dbt projects.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.parser.project.DbtModelConfig
   cosmos.providers.dbt.parser.project.DbtModel
   cosmos.providers.dbt.parser.project.DbtProject




Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.parser.project.logger


.. py:data:: logger



.. py:class:: DbtModelConfig

   Represents a single model config.

   .. py:attribute:: config_types
      :type: ClassVar[List[str]]
      :value: ['materialized', 'schema', 'tags']



   .. py:attribute:: config_selectors
      :type: Set[str]



   .. py:attribute:: upstream_models
      :type: Set[str]



   .. py:method:: __add__(other_config: DbtModelConfig) -> DbtModelConfig

      Add one config to another. Necessary because configs can come from different places


   .. py:method:: _config_selector_ooo(sql_configs: Set[str], properties_configs: Set[str], prefixes: List[str] = None) -> Set[str]

      this will force values from the sql files to override whatever is in the properties.yml. So ooo:
      # 1. model sql files
      # 2. properties.yml files



.. py:class:: DbtModel

   Represents a single dbt model.

   .. py:attribute:: name
      :type: str



   .. py:attribute:: path
      :type: pathlib.Path



   .. py:attribute:: config
      :type: DbtModelConfig



   .. py:method:: __post_init__() -> None

      Parses the file and extracts metadata (dependencies, tags, etc)


   .. py:method:: _extract_config(kwarg, config_name: str)


   .. py:method:: __repr__() -> str

      Returns the string representation of the model.



.. py:class:: DbtProject

   Represents a single dbt project.

   .. py:attribute:: project_name
      :type: str



   .. py:attribute:: dbt_root_path
      :type: str
      :value: '/usr/local/airflow/dbt'



   .. py:attribute:: dbt_models_dir
      :type: str
      :value: 'models'



   .. py:attribute:: models
      :type: Dict[str, DbtModel]



   .. py:attribute:: project_dir
      :type: pathlib.Path



   .. py:attribute:: models_dir
      :type: pathlib.Path



   .. py:method:: __post_init__() -> None

      Initializes the parser.


   .. py:method:: _handle_sql_file(path: pathlib.Path) -> None

      Handles a single sql file.


   .. py:method:: _handle_config_file(path: pathlib.Path) -> None

      Handles a single config file.
