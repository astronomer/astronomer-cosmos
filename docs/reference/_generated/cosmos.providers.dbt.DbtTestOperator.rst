cosmos.providers.dbt.DbtTestOperator
====================================

.. currentmodule:: cosmos.providers.dbt

.. autoclass:: DbtTestOperator

   
   .. automethod:: __init__

   
   .. rubric:: Methods

   .. autosummary::
   
      ~DbtTestOperator.__init__
      ~DbtTestOperator.add_global_flags
      ~DbtTestOperator.add_inlets
      ~DbtTestOperator.add_outlets
      ~DbtTestOperator.build_and_run_cmd
      ~DbtTestOperator.clear
      ~DbtTestOperator.defer
      ~DbtTestOperator.dry_run
      ~DbtTestOperator.exception_handling
      ~DbtTestOperator.execute
      ~DbtTestOperator.expand_mapped_task
      ~DbtTestOperator.get_closest_mapped_task_group
      ~DbtTestOperator.get_dag
      ~DbtTestOperator.get_dbt_path
      ~DbtTestOperator.get_direct_relative_ids
      ~DbtTestOperator.get_direct_relatives
      ~DbtTestOperator.get_env
      ~DbtTestOperator.get_extra_links
      ~DbtTestOperator.get_flat_relative_ids
      ~DbtTestOperator.get_flat_relatives
      ~DbtTestOperator.get_inlet_defs
      ~DbtTestOperator.get_mapped_ti_count
      ~DbtTestOperator.get_outlet_defs
      ~DbtTestOperator.get_parse_time_mapped_ti_count
      ~DbtTestOperator.get_serialized_fields
      ~DbtTestOperator.get_task_instances
      ~DbtTestOperator.get_template_env
      ~DbtTestOperator.has_dag
      ~DbtTestOperator.iter_mapped_dependants
      ~DbtTestOperator.iter_mapped_task_groups
      ~DbtTestOperator.on_kill
      ~DbtTestOperator.partial
      ~DbtTestOperator.post_execute
      ~DbtTestOperator.pre_execute
      ~DbtTestOperator.prepare_for_execution
      ~DbtTestOperator.prepare_template
      ~DbtTestOperator.render_template
      ~DbtTestOperator.render_template_fields
      ~DbtTestOperator.resolve_template_files
      ~DbtTestOperator.run
      ~DbtTestOperator.run_command
      ~DbtTestOperator.serialize_for_task_group
      ~DbtTestOperator.set_downstream
      ~DbtTestOperator.set_upstream
      ~DbtTestOperator.set_xcomargs_dependencies
      ~DbtTestOperator.unmap
      ~DbtTestOperator.update_relative
      ~DbtTestOperator.xcom_pull
      ~DbtTestOperator.xcom_push
   
   

   
   
   .. rubric:: Attributes

   .. autosummary::
   
      ~DbtTestOperator.HIDE_ATTRS_FROM_UI
      ~DbtTestOperator.dag
      ~DbtTestOperator.dag_id
      ~DbtTestOperator.deps
      ~DbtTestOperator.downstream_list
      ~DbtTestOperator.end_date
      ~DbtTestOperator.extra_links
      ~DbtTestOperator.global_operator_extra_link_dict
      ~DbtTestOperator.inherits_from_empty_operator
      ~DbtTestOperator.label
      ~DbtTestOperator.leaves
      ~DbtTestOperator.log
      ~DbtTestOperator.node_id
      ~DbtTestOperator.operator_class
      ~DbtTestOperator.operator_extra_link_dict
      ~DbtTestOperator.operator_extra_links
      ~DbtTestOperator.operator_name
      ~DbtTestOperator.output
      ~DbtTestOperator.pool
      ~DbtTestOperator.priority_weight_total
      ~DbtTestOperator.roots
      ~DbtTestOperator.shallow_copy_attrs
      ~DbtTestOperator.start_date
      ~DbtTestOperator.subdag
      ~DbtTestOperator.subprocess_hook
      ~DbtTestOperator.supports_lineage
      ~DbtTestOperator.task_group
      ~DbtTestOperator.task_type
      ~DbtTestOperator.template_ext
      ~DbtTestOperator.template_fields
      ~DbtTestOperator.template_fields_renderers
      ~DbtTestOperator.ui_color
      ~DbtTestOperator.ui_fgcolor
      ~DbtTestOperator.upstream_list
      ~DbtTestOperator.weight_rule
      ~DbtTestOperator.priority_weight
      ~DbtTestOperator.owner
      ~DbtTestOperator.task_id
      ~DbtTestOperator.outlets
      ~DbtTestOperator.inlets
      ~DbtTestOperator.upstream_task_ids
      ~DbtTestOperator.downstream_task_ids
   
   