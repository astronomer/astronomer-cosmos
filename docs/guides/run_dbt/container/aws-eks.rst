.. _aws-eks:

AWS EKS execution mode
=======================

The Amazon Elastic Kubernetes Service (AWS EKS),  ``aws_eks``, approach is very similar to the ``kubernetes`` approach, but it is specifically designed to run on AWS EKS clusters.
It uses the `EKSPodOperator <https://airflow.apache.org/docs/apache-airflow-providers-amazon/8.19.0/operators/eks.html#perform-a-task-on-an-amazon-eks-cluster>`_
to run the dbt commands. You need to provide the ``cluster_name`` in your operator_args to connect to the AWS EKS cluster.


Example Dag

.. code-block:: python

    postgres_password_secret = Secret(
        deploy_type="env",
        deploy_target="POSTGRES_PASSWORD",
        secret="postgres-secrets",
        key="password",
    )

    docker_cosmos_dag = DbtDag(
        # ...
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.AWS_EKS,
        ),
        operator_args={
            "image": "dbt-jaffle-shop:1.0.0",
            "cluster_name": CLUSTER_NAME,
            "get_logs": True,
            "is_delete_operator_pod": False,
            "secrets": [postgres_password_secret],
        },
    )
