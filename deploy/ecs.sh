#!/usr/bin/env bash

if [ -z $1 ]; then
  echo "Usage: $0 [prod|dev]"
  exit 1
else
  ENV=$1
fi

# more bash-friendly output for jq
JQ="jq --raw-output --exit-status"

configure_aws_cli(){
  aws --version
  aws configure set default.region us-east-1
  aws configure set default.output json
}

deploy_cluster() {
  family="opentraffic-datastore"

  make_task_def
  register_definition

  if [[ $(aws ecs update-service --cluster datastore-$ENV --service datastore-$ENV --task-definition $revision | $JQ '.service.taskDefinition') != $revision ]]; then
    echo "Error updating service."
    return 1
  fi

  # wait for older revisions to disappear
  # not really necessary, but nice for demos
  for attempt in {1..30}; do
    if stale=$(aws ecs describe-services --cluster datastore-$ENV --services datastore-$ENV | \
            $JQ ".services[0].deployments | .[] | select(.taskDefinition != \"$revision\") | .taskDefinition"); then
      echo "Waiting for stale deployments:"
      echo "$stale"
      sleep 5
    else
      echo "Deployed!"
      return 0
    fi
  done

  echo "Service update took too long."
  return 1
}

make_task_def(){
  task_template='[
    {
      "name": "opentraffic-reporter",
      "image": "%s.dkr.ecr.us-east-1.amazonaws.com/opentraffic/datastore:%s",
      "essential": true,
      "memory": 1024,
      "cpu": 1024,
      "environment": [
        {
          "name": "POSTGRES_HOST",
          "value": "%s"
        },
        {
          "name": "POSTGRES_USER",
          "value": "%s"
        },
        {
          "name": "POSTGRES_PASSWORD",
          "value": "%s"
        },
        {
          "name": "POSTGRES_DB",
          "value": "%s"
        }
      ],
      "portMappings": [
        {
          "containerPort": 8003,
          "hostPort": 8003
        }
      ]
    }
  ]'

  task_def=$(printf "$task_template" $AWS_ACCOUNT_ID $CIRCLE_SHA1 $POSTGRES_HOST $POSTGRES_USER $POSTGRES_PASSWORD $POSTGRES_DB)
}

push_ecr_image(){
  eval $(aws ecr get-login --region us-east-1)
  docker push $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/opentraffic/datastore:$CIRCLE_SHA1
}

register_definition() {
  if revision=$(aws ecs register-task-definition --container-definitions "$task_def" --family $family | $JQ '.taskDefinition.taskDefinitionArn'); then
    echo "Revision: $revision"
  else
    echo "Failed to register task definition"
    return 1
  fi
}

configure_aws_cli
push_ecr_image
deploy_cluster
