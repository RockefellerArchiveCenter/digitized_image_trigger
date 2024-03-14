#!/usr/bin/env python3

import json
import logging
import traceback
from os import environ

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FORMAT_MAP = {
    'rac-prod-av-upload-audio': 'audio',
    'rac-dev-av-upload-audio': 'audio',
    'rac-prod-av-upload-video': 'video',
    'rac-dev-av-upload-video': 'video',
}
VALIDATION_SERVICE = 'digitized_av_validation'
QC_SERVICE = 'digitized_av_qc'

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
    return ecs_client.run_task(
        cluster=config.get('ECS_CLUSTER'),
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [config.get('ECS_SUBNET')],
                'securityGroups': [],
                'assignPublicIp': 'DISABLED'
            }
        },
        taskDefinition=task_definition,
        count=1,
        startedBy='lambda/digitized_av_trigger',
        overrides={
            'containerOverrides': [
                {
                    "name": task_definition,
                    "environment": environment
                }
            ]
        }
    )


def handle_s3_object_put(config, ecs_client, event):
    """Handles actions for newly created objects in S3 buckets."""

    bucket = event['Records'][0]['s3']['bucket']['name']
    object = event['Records'][0]['s3']['object']['key']
    format = FORMAT_MAP[bucket]

    logger.info(
        "Running validation task for event from object {} in bucket {} with format {}".format(
            object,
            bucket,
            format))

    environment = [
        {
            "name": "FORMAT",
            "value": format
        },
        {
            "name": "AWS_SOURCE_BUCKET",
            "value": bucket
        },
        {
            "name": "SOURCE_FILENAME",
            "value": object
        }
    ]

    return run_task(
        ecs_client,
        config,
        'digitized_av_validation',
        environment)


def handle_qc_approval(config, ecs_client, attributes):
    """Handles QC approval of package."""

    format = attributes['format']['Value']
    refid = attributes['refid']['Value']
    rights_ids = attributes['rights_ids']['Value']

    logger.info(
        "Running packaging task for event from object {} with format {}".format(
            refid,
            format))

    environment = [
        {
            "name": "FORMAT",
            "value": format
        },
        {
            "name": "REFID",
            "value": refid
        },
        {
            "name": "RIGHTS_IDS",
            "value": rights_ids
        }
    ]

    return run_task(
        ecs_client,
        config,
        'digitized_av_packaging',
        environment)


def handle_validation_approval(config, ecs_client):
    """Scales up ECS Service when items are waiting for QC"""
    logger.info("Scaling up QC service.")

    service = ecs_client.describe_services(
        cluster=config.get('ECS_CLUSTER'),
        services=[config.get('QC_ECS_SERVICE')])
    if (len(service['services']) and service['services']
            [0]['desiredCount'] < 1):
        return ecs_client.update_service(
            cluster=config.get('ECS_CLUSTER'),
            service=config.get('QC_ECS_SERVICE'),
            desiredCount=1)


def handle_qc_complete(config, ecs_client):
    """Scales down ECS Service when nothing is left to QC"""
    logger.info("Scaling down QC service.")

    return ecs_client.update_service(
        cluster=config.get('ECS_CLUSTER'),
        service=config.get('QC_ECS_SERVICE'),
        desiredCount=0)


def lambda_handler(event, context):
    """Triggers ECS task."""

    config = get_config(full_config_path)
    ecs_client = boto3.client(
        'ecs',
        region_name=environ.get('AWS_DEFAULT_REGION', 'us-east-1'))

    if event['Records'][0].get('s3'):
        """Handles events from S3 buckets."""

        logger.info(f"Received S3 event {event}")

        event_type = event['Records'][0]['eventName']

        response = f'Nothing to do for S3 event: {event}'

        if event_type in ['ObjectCreated:Put',
                          'ObjectCreated:CompleteMultipartUpload']:
            """Handles object creation events."""
            response = handle_s3_object_put(config, ecs_client, event)

    elif event['Records'][0].get('Sns'):
        """Handles events from SNS."""

        logger.info(f"Received SNS event {event}")

        attributes = event['Records'][0]['Sns']['MessageAttributes']

        response = f'Nothing to do for SNS event: {event}'

        if (attributes['service']['Value'] == VALIDATION_SERVICE):
            if attributes['outcome']['Value'] == 'SUCCESS':
                """Handles QC approval events."""
                response = handle_validation_approval(config, ecs_client)

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
    return json.dumps(response, default=str)
