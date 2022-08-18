# List each AZ`s default subnets, and use it in EMR cluster, therefore EMR will pick the largest capacity AZ to create EC2 instances
data "aws_subnets" "default_subnets" {
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "template_file" "partner_sfn_definition" {
  template = <<EOF
{
  "StartAt": "Create_A_Cluster",
  "States": {
    "Create_A_Cluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:createCluster.sync",
      "Parameters": {
        "Name": "AdvWorkflowCluster",
        "VisibleToAllUsers": true,
        "ReleaseLabel": "emr-6.7.0",
        "Applications": [
          {
            "Name": "Hadoop"
          },
          {
            "Name": "Spark"
          }
        ],
        "ServiceRole": "${aws_iam_role.mrpid_partner_emr_role.id}",
        "JobFlowRole": "${aws_iam_role.mrpid_partner_ec2_role.id}",
        "ManagedScalingPolicy": {
          "ComputeLimits": {
            "MinimumCapacityUnits": 2,
            "MaximumCapacityUnits": 20,
            "UnitType": "InstanceFleetUnits"
          }
        },
        "Instances": {
          "KeepJobFlowAliveWhenNoSteps": true,
          "Ec2SubnetIds": ["${join("\", \"", data.aws_subnets.default_subnets.ids)}"],
          "InstanceFleets": [
            {
              "InstanceFleetType": "MASTER",
              "TargetOnDemandCapacity": 1,
              "InstanceTypeConfigs": [
                {
                  "InstanceType": "m6g.xlarge"
                }
              ]
            },
            {
              "InstanceFleetType": "CORE",
              "TargetOnDemandCapacity": 3,
              "InstanceTypeConfigs": [
                {
                  "InstanceType": "m6g.8xlarge"
                }
              ]
            }
          ]
        },
        "BootstrapActions": [
          {
            "Name": "install-cloudwatch-agent",
            "ScriptBootstrapAction": {
              "Path": "s3://mrpid-partner-${var.md5hash_aws_account_id}-confs/cloudwatch_agent/cloudwatch_agent_install.sh",
              "Args": []
            }
          }
        ]
      },
      "ResultPath": "$.CreateClusterResult",
      "TimeoutSeconds": 600,
      "Next": "Enable_Termination_Protection"
    },
    "Enable_Termination_Protection": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:setClusterTerminationProtection",
      "Parameters": {
        "ClusterId.$": "$.CreateClusterResult.ClusterId",
        "TerminationProtected": true
      },
      "ResultPath": null,
      "TimeoutSeconds": 300,
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "Error_Terminate_Cluster"
        }
      ],
      "Next": "Wait_for_stage_one_ready"
    },
    "Wait_for_stage_one_ready": {
      "Type": "Task",
      "Parameters": {
        "Bucket": "mrpid-publisher-${var.md5hash_aws_account_id}",
        "Key.$": "States.Format('{}/step_1_meta_enc_kc/_SUCCESS', $.instanceId)"
      },
      "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
      "ResultPath": null,
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 30,
          "MaxAttempts": 360,
          "BackoffRate": 1
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "Error_Disable_Termination_Protection"
        }
      ],
      "Next": "Stage_One"
    },
    "Stage_One": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:addStep.sync",
      "Parameters": {
        "ClusterId.$": "$.CreateClusterResult.Cluster.Id",
        "Step": {
          "Name": "The first stage",
          "ActionOnFailure": "TERMINATE_JOB_FLOW",
          "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args.$": "States.Array('bash', '-c', States.Format('spark-submit --deploy-mode cluster --master yarn --jars {} --num-executors 10 --executor-cores 5 --executor-memory 3G --conf spark.driver.memory=10G --conf spark.sql.shuffle.partitions=10 --conf spark.yarn.maxAppAttempts=1 --class com.meta.mr.multikey.partner.PartnerStageOne {} s3://mrpid-publisher-${var.md5hash_aws_account_id}/{} s3://mrpid-partner-${var.md5hash_aws_account_id}/{} {} {} 2>&1 | sudo tee /mnt/var/log/spark/PartnerStageOne.log', $.pidMrMultikeyJarPath, $.pidMrMultikeyJarPath, $.instanceId, $.instanceId, $.outputPath, $.inputPath))"
          }
        }
      },
      "ResultPath": null,
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "Error_Disable_Termination_Protection"
        }
      ],
      "Next": "Wait_for_stage_two_ready"
    },
    "Wait_for_stage_two_ready": {
      "Type": "Task",
      "Parameters": {
        "Bucket": "mrpid-publisher-${var.md5hash_aws_account_id}",
        "Key.$": "States.Format('{}/step_2_adv_unmatched_enc_kc_kp/_SUCCESS', $.instanceId)"
      },
      "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 30,
          "MaxAttempts": 360,
          "BackoffRate": 1
        }
      ],
      "ResultPath": null,
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "Error_Disable_Termination_Protection"
        }
      ],
      "Next": "Stage_Two"
    },
    "Stage_Two": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:addStep.sync",
      "Parameters": {
        "ClusterId.$": "$.CreateClusterResult.Cluster.Id",
        "Step": {
          "Name": "The second stage",
          "ActionOnFailure": "TERMINATE_JOB_FLOW",
          "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args.$": "States.Array('bash', '-c', States.Format('spark-submit --deploy-mode cluster --master yarn --jars {} --num-executors 10 --executor-cores 5 --executor-memory 3G --conf spark.driver.memory=10G --conf spark.sql.shuffle.partitions=10 --conf spark.yarn.maxAppAttempts=1 --class com.meta.mr.multikey.partner.PartnerStageTwo {} s3://mrpid-publisher-${var.md5hash_aws_account_id}/{} s3://mrpid-partner-${var.md5hash_aws_account_id}/{} {} 2>&1 | sudo tee /mnt/var/log/spark/PartnerStageTwo.log', $.pidMrMultikeyJarPath, $.pidMrMultikeyJarPath, $.instanceId, $.instanceId, $.outputPath))"
          }
        }
      },
      "ResultPath": null,
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "Error_Disable_Termination_Protection"
        }
      ],
      "Next": "Disable_Termination_Protection"
    },
    "Disable_Termination_Protection": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:setClusterTerminationProtection",
      "Parameters": {
        "ClusterId.$": "$.CreateClusterResult.Cluster.Id",
        "TerminationProtected": false
      },
      "ResultPath": null,
      "TimeoutSeconds": 300,
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "Error_Terminate_Cluster"
        }
      ],
      "Next": "Terminate_Cluster"
    },
    "Terminate_Cluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:terminateCluster.sync",
      "Parameters": {
        "ClusterId.$": "$.CreateClusterResult.Cluster.Id"
      },
      "TimeoutSeconds": 600,
      "End": true
    },
    "Error_Disable_Termination_Protection": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:setClusterTerminationProtection",
      "Parameters": {
        "ClusterId.$": "$.CreateClusterResult.Cluster.Id",
        "TerminationProtected": false
      },
      "ResultPath": null,
      "TimeoutSeconds": 300,
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "Error_Terminate_Cluster"
        }
      ],
      "Next": "Error_Terminate_Cluster"
    },
    "Error_Terminate_Cluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:terminateCluster.sync",
      "Parameters": {
        "ClusterId.$": "$.CreateClusterResult.Cluster.Id"
      },
      "TimeoutSeconds": 600,
      "Next": "Fail"
    },
    "Fail": {
      "Type": "Fail"
    }
  }
}
EOF
}
