#!/usr/bin/env python3

import json
from pathlib import Path
from unittest.mock import patch

import boto3
from moto import mock_ecs, mock_ssm
from moto.core import DEFAULT_ACCOUNT_ID

from src.handle_digitized_av_trigger import get_config, lambda_handler


@mock_ecs
@patch('src.handle_digitized_av_trigger.get_config')
def test_s3_args(mock_config):
    test_cluster_name = "default"
    mock_config.return_value = {
        "AWS_REGION": "us-east-1",
        "ECS_CLUSTER": test_cluster_name,
        "ECS_SUBNET": "subnet"}
    client = boto3.client("ecs", region_name="us-east-1")
    client.create_cluster(clusterName=test_cluster_name)
    client.register_task_definition(
        family="digitized_av_validation",
        containerDefinitions=[
            {
                "name": "digitized_av_validation",
                "image": "docker/hello-world:latest",
                "cpu": 1024,
                "memory": 400,
            }
        ],
    )

    for fixture, expected_args in [
            ('s3_put_audio.json', 's3_audio_args.json'),
            ('s3_put_video.json', 's3_video_args.json')]:
        with open(Path('fixtures', fixture), 'r') as df:
            message = json.load(df)
            response = json.loads(lambda_handler(message, None))
            assert len(response['tasks']) == 1
            assert response['tasks'][0]['startedBy'] == 'lambda/digitized_av_trigger'
            assert response['tasks'][0][
                'taskDefinitionArn'] == f"arn:aws:ecs:us-east-1:{DEFAULT_ACCOUNT_ID}:task-definition/digitized_av_validation:1"
            with open(Path('fixtures', expected_args), 'r') as af:
                args = json.load(af)
                assert response['tasks'][0]['overrides'] == args


@mock_ecs
@patch('src.handle_digitized_av_trigger.get_config')
def test_sns_args(mock_config):
    test_cluster_name = "default"
    mock_config.return_value = {
        "AWS_REGION": "us-east-1",
        "ECS_CLUSTER": test_cluster_name,
        "ECS_SUBNET": "subnet",
        "QC_ECS_SERVICE": "digitized_av_qc"}
    client = boto3.client("ecs", region_name="us-east-1")
    client.create_cluster(clusterName=test_cluster_name)
    client.register_task_definition(
        family="digitized_av_packaging",
        containerDefinitions=[
            {
                "name": "digitized_av_packaging",
                "image": "docker/hello-world:latest",
                "cpu": 1024,
                "memory": 400,
            }
        ],
    )
    client.create_service(
        cluster=test_cluster_name,
        serviceName='digitized_av_qc'
    )

    for fixture, expected_args in [
            ('sns_audio_accept.json', 'sns_audio_args.json'),
            ('sns_video_accept.json', 'sns_video_args.json')]:
        with open(Path('fixtures', fixture), 'r') as df:
            message = json.load(df)
            response = json.loads(lambda_handler(message, None))
            assert len(response['tasks']) == 1
            assert response['tasks'][0]['startedBy'] == 'lambda/digitized_av_trigger'
            assert response['tasks'][0][
                'taskDefinitionArn'] == f"arn:aws:ecs:us-east-1:{DEFAULT_ACCOUNT_ID}:task-definition/digitized_av_packaging:1"
            with open(Path('fixtures', expected_args), 'r') as af:
                args = json.load(af)
                assert response['tasks'][0]['overrides'] == args

    for fixture in ['sns_audio_reject.json', 'sns_audio_reject.json']:
        with open(Path('fixtures', fixture), 'r') as df:
            message = json.load(df)
            response = json.loads(lambda_handler(message, None))
            assert 'Nothing to do for SNS event:' in response

    with open(Path('fixtures', 'sns_video_valid.json'), 'r') as df:
        created = client.describe_services(services=['digitized_av_qc'])
        assert created['services'][0]['desiredCount'] == 0

        message = json.load(df)
        response = json.loads(lambda_handler(message, None))
        assert response['service']['desiredCount'] == 1

    with open(Path('fixtures', 'sns_complete.json'), 'r') as df:
        client.update_service(
            service='digitized_av_qc',
            desiredCount=1)
        created = client.describe_services(services=['digitized_av_qc'])
        assert created['services'][0]['desiredCount'] == 1

        message = json.load(df)
        response = json.loads(lambda_handler(message, None))
        assert response['service']['desiredCount'] == 0


@mock_ssm
def test_config():
    ssm = boto3.client('ssm', region_name='us-east-1')
    path = "/dev/digitized_av_trigger"
    for name, value in [("foo", "bar"), ("baz", "buzz")]:
        ssm.put_parameter(
            Name=f"{path}/{name}",
            Value=value,
            Type="SecureString",
        )
    config = get_config(path)
    assert config == {'foo': 'bar', 'baz': 'buzz'}
