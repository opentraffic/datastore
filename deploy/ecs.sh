#!/usr/bin/env bash
set -e

usage() {
  echo "Usage: $0 --env [prod|dev] --region [us-east-1] --cpu-reservation [cpu] --mem-reservation [mem]"
  exit 2
}

## get vars: set defaults
MEM=512
CPU=1024
REGION="us-east-1"
ENV="bogus"

if [ -z $1 ]; then
  usage
fi

while [[ $# -gt 0 ]]
do
  case "$1" in
    --env|-e)
      case "$2" in
        'prod'|'dev')
          ENV=$2
		      shift
        ;;

        *)
          usage
          ;;
      esac
      ;;

    --region|-r)
      case "$2" in
        'us-east-1')
          REGION=$2
		      shift
          ;;
        *)
          usage
          ;;
      esac
      ;;

    --cpu-reservation|-c)
		  re='^[0-9]+$'
		  if ! [[ "$2" =~ $re ]]; then
        echo "error: --cpu-reservation needs to be an integer" >&2
        usage
      else
        CPU=$2
		    shift
      fi
      ;;

    --mem-reservation|-m)
		  re='^[0-9]+$'
		  if ! [[ "$2" =~ $re ]]; then
        echo "error: --mem-reservation needs to be an integer" >&2
        usage
      else
        MEM=$2
        shift
      fi
      ;;

    *)
      usage
      ;;
  esac
  shift
done

if [ "$ENV" == "bogus" ]; then
  echo "You must set --env [env] in circle.yml!"
  usage
fi

# more bash-friendly output for jq
JQ="jq --raw-output --exit-status"

configure_aws_cli(){
  aws --version
  aws configure set default.region $REGION
  aws configure set default.output json
}

deploy_cluster() {
  family="opentraffic-datastore-$ENV"

  make_task_def
  register_definition

  if [[ $(aws ecs update-service --cluster datastore-$ENV --service opentraffic-datastore-$ENV --task-definition $revision | $JQ '.service.taskDefinition') != $revision ]]; then
    echo "Error updating service."
    return 1
  fi

  # wait for older revisions to disappear
  # not really necessary, but nice for demos
  for attempt in {1..60}; do
    if stale=$(aws ecs describe-services --cluster datastore-$ENV --services opentraffic-datastore-$ENV | \
            $JQ ".services[0].deployments | .[] | select(.taskDefinition != \"$revision\") | .taskDefinition"); then
      echo "Waiting for stale deployments:"
      echo "$stale"
      sleep 10
    else
      echo "Deployed!"
      echo "Writing deployment metric to Cloudwatch."
      aws cloudwatch put-metric-data --metric-name deploy-succeeded --namespace datastore/$ENV --value 1
      return 0
    fi
  done

  echo "Service update took too long."
  echo "Writing deployment metric to Cloudwatch."
  aws cloudwatch put-metric-data --metric-name deploy-failed --namespace datastore/$ENV --value 1
  return 1
}

make_task_def(){
  task_template='[
    {
      "name": "opentraffic-datastore-%s",
      "image": "%s.dkr.ecr.%s.amazonaws.com/opentraffic/datastore-%s:%s",
      "essential": true,
      "memoryReservation": %s,
      "cpu": %s,
      "logConfiguration": {
        "logDriver": "awslogs",
          "options": {
          "awslogs-group": "datastore-%s",
          "awslogs-region": "%s"
        }
      },
      "environment": [
        {
          "name": "POSTGRES_HOST",
          "value": "%s"
        },
        {
          "name": "POSTGRES_PORT",
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
          "hostPort": 0
        }
      ]
    }
  ]'

  # figure out vars per env
  pg_host_raw=$(echo $`printf $ENV`_POSTGRES_HOST)
  pg_host=$(eval echo $pg_host_raw)

  pg_port_raw=$(echo $`printf $ENV`_POSTGRES_PORT)
  pg_port=$(eval echo $pg_port_raw)

  pg_db_raw=$(echo $`printf $ENV`_POSTGRES_DB)
  pg_db=$(eval echo $pg_db_raw)

  pg_user_raw=$(echo $`printf $ENV`_POSTGRES_USER)
  pg_user=$(eval echo $pg_user_raw)

  pg_password_raw=$(echo $`printf $ENV`_POSTGRES_PASSWORD)
  pg_password=$(eval echo $pg_password_raw)

  task_def=$(printf "$task_template" $ENV $AWS_ACCOUNT_ID $REGION $ENV $CIRCLE_SHA1 $MEM $CPU $ENV $REGION $pg_host $pg_port $pg_user $pg_password $pg_db)
}

push_ecr_image(){
  eval $(aws ecr get-login --region $REGION)
  docker tag datastore:latest $AWS_ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/opentraffic/datastore-$ENV:$CIRCLE_SHA1
  docker push $AWS_ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/opentraffic/datastore-$ENV:$CIRCLE_SHA1
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
