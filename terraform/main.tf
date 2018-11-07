provider "aws" {
  region                  = "us-east-1"
  shared_credentials_file = "~/.aws/credentials"
  profile                 = "de_training"
}

module "iam" {
  source = "./iam"
  environment = "Sandbox"
}

module "network" {
  source = "./network"
  project = "DE Training Recife"
  environment = "Sandbox"
  ips_allowed = ["189.3.135.146/32"]
}

module "spark_facilitador" {
  source = "./spark"
  project = "DE Training Recife"
  environment = "Sandbox"
  name = "facilitador"
  vpc_id = "${module.network.vpc_id}"
  subnet_id = "${module.network.subnet_id}"
  configurations = ""
  key_name = "de-training-recife"
  bootstrap_name = "null"
  bootstrap_uri = "null"
  log_uri = "s3://com.thoughtworks.training.de.recife/facilitador/logs"

  sg_emr_master_id = "${module.network.sg_emr_master_id}"
  sg_emr_slave_id = "${module.network.sg_emr_slave_id}"
  emr_ec2_instance_profile_arn = "${module.iam.emr_ec2_instance_profile_arn}"
  emr_service_role_arn = "${module.iam.emr_service_role_arn}"
}

module "spark_par_01" {
  source = "./spark"
  project = "DE Training Recife"
  environment = "Sandbox"
  name = "par_01"
  vpc_id = "${module.network.vpc_id}"
  subnet_id = "${module.network.subnet_id}"
  configurations = ""
  key_name = "de-training-recife"
  bootstrap_name = "null"
  bootstrap_uri = "null"
  log_uri = "s3://com.thoughtworks.training.de.recife/par_01/logs"

  sg_emr_master_id = "${module.network.sg_emr_master_id}"
  sg_emr_slave_id = "${module.network.sg_emr_slave_id}"
  emr_ec2_instance_profile_arn = "${module.iam.emr_ec2_instance_profile_arn}"
  emr_service_role_arn = "${module.iam.emr_service_role_arn}"
}
