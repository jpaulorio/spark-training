#
# EMR resources
#
resource "aws_emr_cluster" "cluster" {
  name           = "${var.name}"
  release_label  = "${var.release_label}"
  applications   = "${var.applications}"
  configurations = "${var.configurations}"

  ec2_attributes {
    key_name                          = "${var.key_name}"
    subnet_id                         = "${var.subnet_id}"
    emr_managed_master_security_group = "${var.sg_emr_master_id}"
    emr_managed_slave_security_group  = "${var.sg_emr_slave_id}"
    instance_profile                  = "${var.emr_ec2_instance_profile_arn}"
  }

  instance_group = "${var.instance_groups}"

#  bootstrap_action {
#    path = "${var.bootstrap_uri}"
#    name = "${var.bootstrap_name}"
#    args = "${var.bootstrap_args}"
#  }

  log_uri      = "${var.log_uri}"
  service_role = "${var.emr_service_role_arn}"

  tags {
    Name        = "${var.name}"
    Project     = "${var.project}"
    Environment = "${var.environment}"
  }
}
