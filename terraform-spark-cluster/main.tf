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
  project = "DE Training Recife - Facilitador"
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
  project = "DE Training Recife - PAR 01"
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

module "spark_par_02" {
  source = "./spark"
  project = "DE Training Recife - PAR 02"
  environment = "Sandbox"
  name = "par_02"
  vpc_id = "${module.network.vpc_id}"
  subnet_id = "${module.network.subnet_id}"
  configurations = ""
  key_name = "de-training-recife"
  bootstrap_name = "null"
  bootstrap_uri = "null"
  log_uri = "s3://com.thoughtworks.training.de.recife/par_02/logs"

  sg_emr_master_id = "${module.network.sg_emr_master_id}"
  sg_emr_slave_id = "${module.network.sg_emr_slave_id}"
  emr_ec2_instance_profile_arn = "${module.iam.emr_ec2_instance_profile_arn}"
  emr_service_role_arn = "${module.iam.emr_service_role_arn}"
}

module "spark_par_03" {
  source = "./spark"
  project = "DE Training Recife - PAR 03"
  environment = "Sandbox"
  name = "par_03"
  vpc_id = "${module.network.vpc_id}"
  subnet_id = "${module.network.subnet_id}"
  configurations = ""
  key_name = "de-training-recife"
  bootstrap_name = "null"
  bootstrap_uri = "null"
  log_uri = "s3://com.thoughtworks.training.de.recife/par_03/logs"

  sg_emr_master_id = "${module.network.sg_emr_master_id}"
  sg_emr_slave_id = "${module.network.sg_emr_slave_id}"
  emr_ec2_instance_profile_arn = "${module.iam.emr_ec2_instance_profile_arn}"
  emr_service_role_arn = "${module.iam.emr_service_role_arn}"
}

module "spark_par_04" {
  source = "./spark"
  project = "DE Training Recife - PAR 04"
  environment = "Sandbox"
  name = "par_04"
  vpc_id = "${module.network.vpc_id}"
  subnet_id = "${module.network.subnet_id}"
  configurations = ""
  key_name = "de-training-recife"
  bootstrap_name = "null"
  bootstrap_uri = "null"
  log_uri = "s3://com.thoughtworks.training.de.recife/par_04/logs"

  sg_emr_master_id = "${module.network.sg_emr_master_id}"
  sg_emr_slave_id = "${module.network.sg_emr_slave_id}"
  emr_ec2_instance_profile_arn = "${module.iam.emr_ec2_instance_profile_arn}"
  emr_service_role_arn = "${module.iam.emr_service_role_arn}"
}

module "spark_par_05" {
  source = "./spark"
  project = "DE Training Recife - PAR 05"
  environment = "Sandbox"
  name = "par_05"
  vpc_id = "${module.network.vpc_id}"
  subnet_id = "${module.network.subnet_id}"
  configurations = ""
  key_name = "de-training-recife"
  bootstrap_name = "null"
  bootstrap_uri = "null"
  log_uri = "s3://com.thoughtworks.training.de.recife/par_05/logs"

  sg_emr_master_id = "${module.network.sg_emr_master_id}"
  sg_emr_slave_id = "${module.network.sg_emr_slave_id}"
  emr_ec2_instance_profile_arn = "${module.iam.emr_ec2_instance_profile_arn}"
  emr_service_role_arn = "${module.iam.emr_service_role_arn}"
}

module "spark_par_06" {
  source = "./spark"
  project = "DE Training Recife - PAR 06"
  environment = "Sandbox"
  name = "par_06"
  vpc_id = "${module.network.vpc_id}"
  subnet_id = "${module.network.subnet_id}"
  configurations = ""
  key_name = "de-training-recife"
  bootstrap_name = "null"
  bootstrap_uri = "null"
  log_uri = "s3://com.thoughtworks.training.de.recife/par_06/logs"

  sg_emr_master_id = "${module.network.sg_emr_master_id}"
  sg_emr_slave_id = "${module.network.sg_emr_slave_id}"
  emr_ec2_instance_profile_arn = "${module.iam.emr_ec2_instance_profile_arn}"
  emr_service_role_arn = "${module.iam.emr_service_role_arn}"
}