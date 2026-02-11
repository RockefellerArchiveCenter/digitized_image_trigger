#!/usr/bin/env python3

import json
from pathlib import Path
from unittest.mock import ANY, patch

import boto3
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID

from src.handle_digitized_image_trigger import (calculate_gb_needed,
                                                get_config, lambda_handler)

CLUSTER_NAME = "default"
CONFIG_DEFAULTS = {
    "AWS_REGION": "us-east-1",
    "ECS_CLUSTER": "default",
    "ECS_CONTAINER_NAME": "digitized_image_qc",
    "ECS_SUBNET": "subnet",
    "QC_ECS_SERVICE": "digitized_image_qc",
    "EBS_STORAGE_MOUNT_PATH": "/ebs",
    "EBS_VOLUME_ROLE": "arn:aws:iam:role/123456789",
    "ECS_SECURITY_GROUP": "sg-123456789",
    "EPHEMERAL_STORAGE_LIMIT": "198",
    "EPHEMERAL_STORAGE_MOUNT_PATH": "/tmp",
    "WAIT_DELAY": "5",
    "WAIT_MAX_ATTEMPTS": "30",
    "EXPANSION_RATIO": "1.5"}


@mock_aws
@patch('src.handle_digitized_image_trigger.get_config')
def test_s3_args(mock_config):
    mock_config.return_value = CONFIG_DEFAULTS
    client = boto3.client("ecs", region_name="us-east-1")
    client.create_cluster(clusterName=CLUSTER_NAME)
    client.register_task_definition(
        family="digitized_image_validation",
        containerDefinitions=[
            {
                "name": "digitized_image_validation",
                "image": "docker/hello-world:latest",
                "cpu": 1024,
                "memory": 400,
            }
        ],
    )

    with open(Path('fixtures', 's3_put.json'), 'r') as df:
        message = json.load(df)
        lambda_handler(message, None)

        tasks = client.list_tasks(cluster=CLUSTER_NAME)
        assert len(tasks['taskArns']) == 1

        task_response = client.describe_tasks(
            cluster=CLUSTER_NAME,
            tasks=[tasks['taskArns'][0]])

        assert task_response['tasks'][0]['startedBy'] == 'lambda/digitized_image_trigger'
        assert task_response['tasks'][0][
            'taskDefinitionArn'] == f"arn:aws:ecs:us-east-1:{DEFAULT_ACCOUNT_ID}:task-definition/digitized_image_validation:1"
        with open(Path('fixtures', 's3_args.json'), 'r') as af:
            args = json.load(af)
            assert task_response['tasks'][0]['overrides'] == args


@mock_aws
@patch('src.handle_digitized_image_trigger.get_config')
@patch('src.handle_digitized_image_trigger.execute_service_command')
def test_sns_args(mock_execute_command, mock_config):
    mock_config.return_value = CONFIG_DEFAULTS
    client = boto3.client("ecs", region_name="us-east-1")
    client.create_cluster(clusterName=CLUSTER_NAME)
    client.register_task_definition(
        family="digitized_image_packaging",
        containerDefinitions=[
            {
                "name": "digitized_image_packaging",
                "image": "docker/hello-world:latest",
                "cpu": 1024,
                "memory": 400,
            }
        ],
    )
    client.create_service(
        cluster=CLUSTER_NAME,
        serviceName='digitized_image_qc'
    )

    with open(Path('fixtures', 'sns_accept.json'), 'r') as df:
        message = json.load(df)
        lambda_handler(message, None)

        tasks = client.list_tasks(cluster=CLUSTER_NAME)
        assert len(tasks['taskArns']) == 1

        task_response = client.describe_tasks(
            cluster=CLUSTER_NAME,
            tasks=[tasks['taskArns'][0]])

        assert task_response['tasks'][0]['startedBy'] == 'lambda/digitized_image_trigger'
        assert task_response['tasks'][0][
            'taskDefinitionArn'] == f"arn:aws:ecs:us-east-1:{DEFAULT_ACCOUNT_ID}:task-definition/digitized_image_packaging:1"
        with open(Path('fixtures', 'sns_args.json'), 'r') as af:
            args = json.load(af)
            assert task_response['tasks'][0]['overrides'] == args

    with open(Path('fixtures', 'sns_reject.json'), 'r') as df:
        message = json.load(df)
        lambda_handler(message, None)

        tasks = client.list_tasks(cluster=CLUSTER_NAME)
        assert len(tasks['taskArns']) == 1

    with open(Path('fixtures', 'sns_valid.json'), 'r') as df:
        created = client.describe_services(services=['digitized_image_qc'])
        assert created['services'][0]['desiredCount'] == 0

        message = json.load(df)
        lambda_handler(message, None)
        updated = client.describe_services(services=['digitized_image_qc'])
        assert updated['services'][0]['desiredCount'] == 1
        mock_execute_command.assert_called_once_with(
            ANY,
            created['services'][0]['clusterArn'],
            'digitized_image_qc',
            'python manage.py discover_packages 20f8da26e268418ead4aa2365f816a08 0f880ce8-a9fa-401e-b954-2d1e7e9a91c6 R898/20f8da26e268418ead4aa2365f816a08.tar.gz',
            True,
            ANY)

    with open(Path('fixtures', 'sns_complete.json'), 'r') as df:
        client.update_service(
            service='digitized_image_qc',
            desiredCount=1)
        created = client.describe_services(services=['digitized_image_qc'])
        assert created['services'][0]['desiredCount'] == 1

        message = json.load(df)
        lambda_handler(message, None)
        complete = client.describe_services(services=['digitized_image_qc'])
        assert complete['services'][0]['desiredCount'] == 0


@mock_aws
def test_config():
    ssm = boto3.client('ssm', region_name='us-east-1')
    path = "/dev/digitized_image_trigger"
    for name, value in [("foo", "bar"), ("baz", "buzz")]:
        ssm.put_parameter(
            Name=f"{path}/{name}",
            Value=value,
            Type="SecureString",
        )
    config = get_config(path)
    assert config == {'foo': 'bar', 'baz': 'buzz'}


def test_calculate_gb_needed():
    """Asserts GB needed are correctly calculated."""
    for input, expected in [
            (1000000000, 3),
            (1900000000, 5),
            (3900000000, 10)]:
        output = calculate_gb_needed(input, 1.5)
        assert output == expected
    for input, expected in [
            (1000000000, 3),
            (1900000000, 6),
            (3900000000, 11)]:
        output = calculate_gb_needed(input)
        assert output == expected
