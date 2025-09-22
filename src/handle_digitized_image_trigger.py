#!/usr/bin/env python3

import logging
import traceback
from os import environ

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

VALIDATION_SERVICE = 'digitized_image_validation'
QC_SERVICE = 'digitized_image_qc'
PACKAGING_SERVICE = 'digitized_image_packaging'
VALIDATION_SERVICE = 'digitized_image_validation'

full_config_path = f"/{environ.get('ENV')}/{environ.get('APP_CONFIG_PATH')}"


def get_config(ssm_parameter_path):
    """Fetch config values from Parameter Store.

    Args:
        ssm_parameter_path (str): Path to parameters

    Returns:
        configuration (dict): all parameters found at the supplied path.
    """
    configuration = {}
    try:
        ssm_client = boto3.client(
            'ssm',
            region_name=environ.get('AWS_DEFAULT_REGION', 'us-east-1'))

        param_details = ssm_client.get_parameters_by_path(
            Path=ssm_parameter_path,
            Recursive=False,
            WithDecryption=True)

        for param in param_details.get('Parameters', []):
            param_path_array = param.get('Name').split("/")
            section_position = len(param_path_array) - 1
            section_name = param_path_array[section_position]
            configuration[section_name] = param.get('Value')

    except BaseException:
        print("Encountered an error loading config from SSM.")
        traceback.print_exc()
    finally:
        return configuration


def run_task(ecs_client, config, task_definition, environment):
    response = ecs_client.run_task(
        cluster=config.get('ECS_CLUSTER'),
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [config.get('ECS_SUBNET')],
                'securityGroups': [config.get('ECS_SECURITY_GROUP')],
                'assignPublicIp': 'DISABLED'
            }
        },
        taskDefinition=task_definition,
        count=1,
        startedBy='lambda/digitized_image_trigger',
        overrides={
            'containerOverrides': [
                {
                    "name": task_definition,
                    "environment": environment
                }
            ]
        }
    )
    return ", ".join([t['taskArn'] for t in response['tasks']])


def handle_s3_object_put(config, ecs_client, event):
    """Handles actions for newly created objects in S3 buckets."""

    bucket = event['Records'][0]['s3']['bucket']['name']
    object = event['Records'][0]['s3']['object']['key']

    logger.info(
        "Running validation task for event from object {} in bucket {}".format(
            object,
            bucket))

    environment = [
        {
            "name": "AWS_SOURCE_BUCKET",
            "value": bucket
        },
        {
            "name": "SOURCE_FILENAME",
            "value": object
        }
    ]

    task_id = run_task(
        ecs_client,
        config,
        VALIDATION_SERVICE,
        environment)
    return f"Task {task_id} with definition {VALIDATION_SERVICE} started for package {object}."


def handle_qc_approval(config, ecs_client, attributes):
    """Handles QC approval of package."""

    refid = attributes['refid']['Value']
    rights_ids = attributes['rights_ids']['Value']

    logger.info(
        "Running packaging task for event from object {}".format(
            refid))

    environment = [
        {
            "name": "REFID",
            "value": refid
        },
        {
            "name": "RIGHTS_IDS",
            "value": rights_ids
        }
    ]

    task_id = run_task(
        ecs_client,
        config,
        PACKAGING_SERVICE,
        environment)
    return f"Task {task_id} with definition {PACKAGING_SERVICE} started for package {refid}."


def handle_validation_approval(config, ecs_client, attributes):
    """Scales up ECS Service when items are waiting for QC"""
    refid = attributes['refid']['Value']

    resp = ecs_client.describe_services(
        cluster=config.get('ECS_CLUSTER'),
        services=[config.get('QC_ECS_SERVICE')])
    if (len(resp['services']) and resp['services']
            [0]['desiredCount'] < 1):
        logger.info("Scaling up QC service.")
        resp = ecs_client.update_service(
            cluster=config.get('ECS_CLUSTER'),
            service=config.get('QC_ECS_SERVICE'),
            desiredCount=1)
        service = resp['service']
    else:
        logger.info("QC service already running.")
        service = resp['services'][0]

    waiter = ecs_client.get_waiter('services_stable')
    waiter.wait(
        cluster=config.get('ECS_CLUSTER'),
        services=[config.get('QC_ECS_SERVICE')],
        WaiterConfig={
            'Delay': 5,  # Poll every 5 seconds
            'MaxAttempts': 30  # Maximum 30 attempts
        }
    )

    tasks = ecs_client.list_tasks(
        cluster=config.get('ECS_CLUSTER'),
        serviceName=config.get('QC_ECS_SERVICE'),
        desiredStatus='RUNNING')

    task_arn = tasks['taskArns'][0]

    execute_service_command(
        ecs_client,
        service['clusterArn'],
        f'python manage.py discover_packages {refid}',
        True,
        task_arn)

    logger.info("Package discovery command executed.")
    return "QC service started and package discovered."


def execute_service_command(
        ecs_client, cluster, command, interactive, task_arn):
    """Executes a command in a running service."""
    ecs_client.execute_command(
        cluster=cluster,
        command=command,
        interactive=interactive,
        task=task_arn)


def handle_qc_complete(config, ecs_client):
    """Scales down ECS Service when nothing is left to QC"""
    logger.info("Scaling down QC service.")

    ecs_client.update_service(
        cluster=config.get('ECS_CLUSTER'),
        service=config.get('QC_ECS_SERVICE'),
        desiredCount=0)

    return "QC service scaled down."


def lambda_handler(event, context):
    """Triggers ECS task."""

    config = get_config(full_config_path)
    ecs_client = boto3.client(
        'ecs',
        region_name=environ.get('AWS_DEFAULT_REGION', 'us-east-1'))

    if event['Records'][0].get('s3'):
        """Handles events from S3 buckets."""

        logger.info("Received S3 event")

        event_type = event['Records'][0]['eventName']

        response = 'Nothing to do for S3 event'

        if event_type in ['ObjectCreated:Put',
                          'ObjectCreated:CompleteMultipartUpload']:
            """Handles object creation events."""
            response = handle_s3_object_put(config, ecs_client, event)

    elif event['Records'][0].get('Sns'):
        """Handles events from SNS."""

        logger.info("Received SNS event")

        attributes = event['Records'][0]['Sns']['MessageAttributes']

        response = 'Nothing to do for SNS event'

        if (attributes['service']['Value'] == VALIDATION_SERVICE):
            if attributes['outcome']['Value'] == 'SUCCESS':
                """Handles QC approval events."""
                response = handle_validation_approval(
                    config, ecs_client, attributes)

        if (attributes['service']['Value'] == QC_SERVICE):
            if attributes['outcome']['Value'] == 'SUCCESS':
                """Handles QC approval events."""
                response = handle_qc_approval(config, ecs_client, attributes)
            elif attributes['outcome']['Value'] == 'COMPLETE':
                """Handles completion of QC."""
                response = handle_qc_complete(config, ecs_client)

    else:
        raise Exception('Unsure how to parse message')

    logger.info(response)
