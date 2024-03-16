#!/usr/bin/env python3

import json
from pathlib import Path
from unittest.mock import patch

import boto3
from moto import mock_ecs, mock_ssm
from moto.core import DEFAULT_ACCOUNT_ID

from src.handle_digitized_image_trigger import get_config, lambda_handler


@mock_ecs
@patch('src.handle_digitized_image_trigger.get_config')
def test_s3_args(mock_config):
    test_cluster_name = "default"
    mock_config.return_value = {
        "AWS_REGION": "us-east-1",
        "ECS_CLUSTER": test_cluster_name,
        "ECS_SUBNET": "subnet"}
    client = boto3.client("ecs", region_name="us-east-1")
    client.create_cluster(clusterName=test_cluster_name)
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
        response = json.loads(lambda_handler(message, None))
        assert len(response['tasks']) == 1
        assert response['tasks'][0]['startedBy'] == 'lambda/digitized_image_trigger'
        assert response['tasks'][0][
            'taskDefinitionArn'] == f"arn:aws:ecs:us-east-1:{DEFAULT_ACCOUNT_ID}:task-definition/digitized_image_validation:1"
        with open(Path('fixtures', 's3_args.json'), 'r') as af:
            args = json.load(af)
            assert response['tasks'][0]['overrides'] == args


@mock_ecs
@patch('src.handle_digitized_image_trigger.get_config')
def test_sns_args(mock_config):
    test_cluster_name = "default"
    mock_config.return_value = {
        "AWS_REGION": "us-east-1",
        "ECS_CLUSTER": test_cluster_name,
        "ECS_SUBNET": "subnet",
        "QC_ECS_SERVICE": "digitized_image_qc"}
    client = boto3.client("ecs", region_name="us-east-1")
    client.create_cluster(clusterName=test_cluster_name)
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
        cluster=test_cluster_name,
        serviceName='digitized_image_qc'
    )

    with open(Path('fixtures', 'sns_accept.json'), 'r') as df:
        message = json.load(df)
        response = json.loads(lambda_handler(message, None))
        assert len(response['tasks']) == 1
        assert response['tasks'][0]['startedBy'] == 'lambda/digitized_image_trigger'
        assert response['tasks'][0][
            'taskDefinitionArn'] == f"arn:aws:ecs:us-east-1:{DEFAULT_ACCOUNT_ID}:task-definition/digitized_image_packaging:1"
        with open(Path('fixtures', 'sns_args.json'), 'r') as af:
            args = json.load(af)
            assert response['tasks'][0]['overrides'] == args

    with open(Path('fixtures', 'sns_reject.json'), 'r') as df:
        message = json.load(df)
        response = json.loads(lambda_handler(message, None))
        assert 'Nothing to do for SNS event:' in response

    with open(Path('fixtures', 'sns_valid.json'), 'r') as df:
        created = client.describe_services(services=['digitized_image_qc'])
        assert created['services'][0]['desiredCount'] == 0

        message = json.load(df)
        response = json.loads(lambda_handler(message, None))
        assert response['service']['desiredCount'] == 1

    with open(Path('fixtures', 'sns_complete.json'), 'r') as df:
        client.update_service(
            service='digitized_image_qc',
            desiredCount=1)
        created = client.describe_services(services=['digitized_image_qc'])
        assert created['services'][0]['desiredCount'] == 1

        message = json.load(df)
        response = json.loads(lambda_handler(message, None))
        assert response['service']['desiredCount'] == 0


@mock_ssm
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
