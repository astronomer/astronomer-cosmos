.. title:: Getting Started with Astronomer Cosmos on AWS ECS

Getting Started with Astronomer Cosmos on AWS ECS
==================================================

Astronomer Cosmos provides a unified way to run containerized workloads across multiple cloud providers. In this guide, you’ll learn how to deploy and run a Cosmos job on AWS Elastic Container Service (ECS) using Fargate.

Prerequisites
+++++++++++++

Before you begin, ensure you have the following:

- An active **AWS Account** with permissions to create ECS clusters, register task definitions, and run tasks.
- The **AWS CLI** installed and configured with the proper credentials.
- **Docker** installed for building your container image.
- Access to your container registry (for example, **Amazon ECR**) where your job image is stored.
- Basic familiarity with AWS ECS concepts (clusters, task definitions, services, and Fargate).
- An existing installation of **Astronomer Cosmos** (refer to the `Cosmos documentation <https://docs.astronomer.io/cosmos/>`_ for more details).



Step-by-step guide
++++++++++++++++++

**Install Airflow and Cosmos**

Create a python virtualenv, activate it, upgrade pip to the latest version and install ``apache airflow`` & ``astronomer cosmos``:

.. code-block:: bash

    python3 -m venv venv
    source venv/bin/activate
    python3 -m pip install --upgrade pip
    pip install apache-airflow
    pip install "astronomer-cosmos[amazon]"
    pip install "aiobotocore[boto3]"
.. note::
   The package aiobotocore[boto3] is optional; you will need it if you plan to use **deferred tasks**.

**Set up your ECR**

1. **Set your secrets**  
   On the `cosmos-examples <https://github.com/astronomer/cosmos-example.git>`_ repository, you can find a ready-to-use Docker image for the AWS ECS service. Just replace your secrets, or you can create your own.

2. **AWS CLI login**  
   Before building and pushing your image, you first need to log in to the AWS service using the AWS CLI tool.  
   Use the following command:

   .. code-block:: bash

      aws ecr-public get-login-password --region <YOUR_REGION> | docker login --username AWS --password-stdin <YOUR_ECS_PASSWORD>

3. **Build and tag your image**  
   Once you have your image ready, run the following commands:

   .. code-block:: bash

      docker build -f Dockerfile.aws_ecs . --platform=linux/amd64 -t <LOCAL_IMAGE_NAME>
      docker tag <YOUR_LOCAL_IMAGE_NAME> <YOUR_ECR_REPOSITORY_URI>

4. **Push your image**  

   .. code-block:: bash

      docker push <YOUR_ECR_REPOSITORY_URI>

**Configure Your AWS Environment**

1. **Create an ECS Cluster**

   Create an ECS cluster to host your Cosmos jobs. You can do this from the AWS Console or using the AWS CLI:

   .. code-block:: bash

      aws ecs create-cluster --cluster-name my-cosmos-cluster

2. **Set Up an IAM Role for ECS Tasks**

   Ensure you have an IAM role that your ECS tasks can assume. This role should include permissions for ECS, ECR, and CloudWatch (for logs). For example, you might create a role named ``ecsTaskExecutionRole`` with the managed policies:

   - ``AmazonECSTaskExecutionRolePolicy``
   - (Optional) Additional policies for custom resource access

3. **Configure Networking**

   For Fargate tasks, make sure you have at least one subnet (preferably in multiple Availability Zones) and a security group that permits outbound internet access if needed. Note the subnet IDs for later use.

**Prepare Your Cosmos Job Definition**

Cosmos jobs are defined as container tasks. Create a task definition file (e.g., ``cosmos-task-definition.json``) with the configuration for your job.

For example:

.. code-block:: json

   {
     "family": "cosmos-job",
     "networkMode": "awsvpc",
     "requiresCompatibilities": [
       "FARGATE"
     ],
     "cpu": "512",
     "memory": "1024",
     "executionRoleArn": "arn:aws:iam::<YOUR_ACCOUNT_ID>:role/ecsTaskExecutionRole",
     "containerDefinitions": [
       {
         "name": "cosmos-job",
         "image": "<YOUR_ECR_REPOSITORY_URI>/your_image:latest",
         "essential": true,
         "environment": [
           { "name": "VAR1", "value": "value1" },
           { "name": "VAR2", "value": "value2" }
         ],
         "logConfiguration": {
           "logDriver": "awslogs",
           "options": {
             "awslogs-group": "/ecs/cosmos-job",
             "awslogs-region": "us-east-1",
             "awslogs-stream-prefix": "ecs"
           }
         }
       }
     ]
   }

.. note::

   Replace ``<YOUR_ACCOUNT_ID>``, ``<YOUR_ECR_REPOSITORY_URI>``, and adjust the CPU, memory, and environment variables as needed.

**Deploy Your Cosmos Job on AWS ECS**

1. **Register the Task Definition**

   Use the AWS CLI to register your task definition:

   .. code-block:: bash

      aws ecs register-task-definition --cli-input-json file://cosmos-task-definition.json

2. **Run the Task**

   Run a test task on your ECS cluster. Specify the subnets and security groups in your network configuration. For example:

   .. code-block:: bash

      aws ecs run-task \
        --cluster my-cosmos-cluster \
        --launch-type FARGATE \
        --task-definition cosmos-job \
        --network-configuration "awsvpcConfiguration={subnets=[subnet-12345678,subnet-87654321],securityGroups=[sg-abcdef12],assignPublicIp=ENABLED}"


**Monitor and Debug Your Job**

1. **Check Task Status**

   You can view the status of your task from the AWS Console under your ECS cluster or via the CLI:

   .. code-block:: bash

      aws ecs describe-tasks --cluster my-cosmos-cluster --tasks <TASK_ID>

2. **View Logs**

   Since the task definition configures AWS CloudWatch Logs, you can view your job’s output in the CloudWatch Logs console. Look for log streams with the prefix you set (e.g., ``ecs/cosmos-job``).

**Conclusion**


By following this guide, you can deploy Astronomer Cosmos jobs on AWS ECS using Fargate. This integration enables you to leverage the scalability and managed infrastructure of ECS while maintaining a consistent container orchestration experience with Cosmos.

For more detailed information on AWS ECS, please refer to the `AWS ECS Developer Guide <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/Welcome.html>`_.

Happy deploying! :rocket:

