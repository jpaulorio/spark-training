variable "project" {
  default = "Unknown"
}

variable "environment" {
  default = "Unknown"
}

variable "name" {}

variable "vpc_id" {}

variable "release_label" {
  default = "emr-5.18.0"
}

variable "applications" {
  default = ["Spark", "Hadoop", "Hive", "Zeppelin"]
  type    = "list"
}

variable "configurations" {}

variable "key_name" {}

variable "subnet_id" {}

variable "instance_groups" {
  default = [
    {
      name           = "MasterInstanceGroup"
      instance_role  = "MASTER"
      instance_type  = "m4.large"
      instance_count = 1
    },
    {
      name           = "CoreInstanceGroup"
      instance_role  = "CORE"
      instance_type  = "m4.large"
      instance_count = "2"
#      bid_price      = "0.30"
    },
  ]

  type = "list"
}

variable "bootstrap_name" {}

variable "bootstrap_uri" {}

variable "bootstrap_args" {
  default = []
  type    = "list"
}

variable "log_uri" {}

variable "sg_emr_master_id" {}
variable "sg_emr_slave_id" {}
variable "emr_ec2_instance_profile_arn" {}
variable "emr_service_role_arn" {}
