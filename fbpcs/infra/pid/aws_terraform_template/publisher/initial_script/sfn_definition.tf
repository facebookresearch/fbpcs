data "template_file" "publisher_sfn_definition" {
  template = <<EOF
{
  "StartAt": "Create_A_Cluster",
  "States": {
    "Create_A_Cluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:createCluster.sync",
      "Parameters": {
        "Name": "MetaWorkflowCluster",
        "VisibleToAllUsers": true,
        "ReleaseLabel": "emr-6.6.0",
        "Applications": [
          {
            "Name": "Hadoop"
          },
          {
            "Name": "Spark"
          }
        ],
        "ServiceRole": "${aws_iam_role.mrpid_publisher_emr_role.id}",
        "JobFlowRole": "${aws_iam_role.mrpid_publisher_ec2_role.id}",
        "Instances": {
          "KeepJobFlowAliveWhenNoSteps": true,
          "InstanceFleets": [
            {
              "InstanceFleetType": "MASTER",
              "TargetOnDemandCapacity": 1,
              "InstanceTypeConfigs": [
                {
                  "InstanceType.$": "$.masterInstanceType"
                }
              ]
            },
            {
              "InstanceFleetType": "CORE",
              "TargetOnDemandCapacity.$": "$.coreTargetOnDemandCapacity",
              "InstanceTypeConfigs": [
                {
                  "InstanceType.$": "$.coreInstanceType"
                }
              ]
            }
          ]
        },
        "BootstrapActions": [
          {
            "Name": "install-cloudwatch-agent",
            "ScriptBootstrapAction": {
              "Path": "s3://mrpid-publisher-${var.md5hash_partner_account_id}-confs/cloudwatch_agent/cloudwatch_agent_install.sh",
              "Args": []
            }
          }
        ]
      },
      "ResultPath": "$.CreateClusterResult",
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
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "Error_Terminate_Cluster"
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
            "Args.$": "States.Array('bash', '-c', States.Format('spark-submit --deploy-mode cluster --master yarn --jars {} --num-executors {} --executor-cores {} --executor-memory {} --conf spark.driver.memory={} --conf spark.sql.shuffle.partitions={} --conf spark.yarn.maxAppAttempts=1 --class com.meta.mr.multikey.publisher.PubStageOne {} s3://mrpid-publisher-${var.md5hash_partner_account_id}/{} {} {} 2>&1 | sudo tee /mnt/var/log/spark/PubStageOne.log', $.pidMrMultikeyJarPath, $.numExecutors, $.executorCores, $.executorMemory, $.driverMemory, $.sqlShufflePartitions, $.pidMrMultikeyJarPath, $.instanceId, $.outputPath, $.inputPath))"
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
        "Bucket": "mrpid-partner-${var.md5hash_partner_account_id}",
        "Key.$": "States.Format('{}/step_1_meta_enc_kc_kp/_SUCCESS', $.instanceId)"
      },
      "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
      "ResultPath": null,
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 30,
          "MaxAttempts": 180,
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
            "Args.$": "States.Array('bash', '-c', States.Format('spark-submit --deploy-mode cluster --master yarn --jars {} --num-executors {} --executor-cores {} --executor-memory {} --conf spark.driver.memory={} --conf spark.sql.shuffle.partitions={} --conf spark.yarn.maxAppAttempts=1 --class com.meta.mr.multikey.publisher.PubStageTwo {} s3://mrpid-publisher-${var.md5hash_partner_account_id}/{} {} s3://mrpid-partner-${var.md5hash_partner_account_id}/{} 2>&1 | sudo tee /mnt/var/log/spark/PubStageTwo.log', $.pidMrMultikeyJarPath, $.numExecutors, $.executorCores, $.executorMemory, $.driverMemory, $.sqlShufflePartitions, $.pidMrMultikeyJarPath, $.instanceId, $.outputPath, $.instanceId))"
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
      "Next": "Wait_for_stage_three_ready"
    },
    "Wait_for_stage_three_ready": {
      "Type": "Task",
      "Parameters": {
        "Bucket": "mrpid-partner-${var.md5hash_partner_account_id}",
        "Key.$": "States.Format('{}/step_3_meta_all_enc_kc_kp_rc_sc_rp/_SUCCESS', $.instanceId)"
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
      "Next": "Stage_Three"
    },
    "Stage_Three": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:addStep.sync",
      "Parameters": {
        "ClusterId.$": "$.CreateClusterResult.Cluster.Id",
        "Step": {
          "Name": "The third stage",
          "ActionOnFailure": "TERMINATE_JOB_FLOW",
          "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args.$": "States.Array('bash', '-c', States.Format('spark-submit --deploy-mode cluster --master yarn --jars {} --num-executors {} --executor-cores {} --executor-memory {} --conf spark.driver.memory={} --conf spark.sql.shuffle.partitions={} --conf spark.yarn.maxAppAttempts=1 --class com.meta.mr.multikey.publisher.PubStageThree {} {} s3://mrpid-partner-${var.md5hash_partner_account_id}/{} 2>&1 | sudo tee /mnt/var/log/spark/PubStageThree.log', $.pidMrMultikeyJarPath, $.numExecutors, $.executorCores, $.executorMemory, $.driverMemory, $.sqlShufflePartitions, $.pidMrMultikeyJarPath, $.outputPath, $.instanceId))"
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
      "Next": "Fail"
    },
    "Fail": {
      "Type": "Fail"
    }
  }
}
EOF
}
