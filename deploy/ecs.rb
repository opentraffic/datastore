#!/usr/bin/env ruby

load Gem.bin_path('bundler', 'bundle')

require 'aws-sdk-core'
require 'aws-sdk-core/ecs'

ecs = Aws::ECS::Client.new(region: 'us-east-1')

family  = 'opentraffic-datastore-prod'
cluster = 'opentraffic-prod'
service = 'opentraffic-datastore'

deploy_task_definition = ecs.register_task_definition(
  family: family,
  container_definitions: JSON.parse(File.read(File.expand_path(File.dirname(__FILE__)) + '/container-definitions.json'))
).task_definition.task_definition_arn

ecs.update_service(
  cluster: cluster,
  service: service,
  desired_count: 1,
  task_definition: deploy_task_definition
)
