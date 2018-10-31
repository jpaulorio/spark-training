resource "aws_vpc" "spark_cluster" {
  cidr_block       = "10.0.0.0/16"
  instance_tenancy = "dedicated"
  enable_dns_hostnames = true

  tags {
    Name = "spark_cluster"
  }
}

resource "aws_internet_gateway" "spark_cluster_gw" {
  vpc_id = "${aws_vpc.spark_cluster.id}"

  tags {
    Name = "spark_cluster"
  }
}

resource "aws_route" "r" {
  route_table_id            = "${aws_vpc.spark_cluster.main_route_table_id}"
  destination_cidr_block    = "0.0.0.0/0"
  gateway_id = "${aws_internet_gateway.spark_cluster_gw.id}"
}

resource "aws_subnet" "spark_cluster" {
  vpc_id     = "${aws_vpc.spark_cluster.id}"
  cidr_block = "10.0.0.0/16"

  tags {
    Name = "spark_cluster"
  }
}

#
# Security group resources
#
resource "aws_security_group" "emr_master" {
  vpc_id                 = "${aws_vpc.spark_cluster.id}"
  revoke_rules_on_delete = true

  tags {
    Name        = "sgSandboxMaster"
    Project     = "${var.project}"
    Environment = "${var.environment}"
  }
}

resource "aws_security_group" "emr_slave" {
  vpc_id                 = "${aws_vpc.spark_cluster.id}"
  revoke_rules_on_delete = true

  tags {
    Name        = "sgSandboxSlave"
    Project     = "${var.project}"
    Environment = "${var.environment}"
  }
}

resource "aws_security_group_rule" "allow_all_master" {
  type            = "egress"
  from_port       = 0
  to_port         = 65535
  protocol        = "all"
  cidr_blocks     = ["0.0.0.0/0"]

  security_group_id = "${aws_security_group.emr_master.id}"
}

resource "aws_security_group_rule" "allow_all_slaves" {
  type            = "egress"
  from_port       = 0
  to_port         = 65535
  protocol        = "all"
  cidr_blocks     = ["0.0.0.0/0"]

  security_group_id = "${aws_security_group.emr_slave.id}"
}

resource "aws_security_group_rule" "allow_ssh_from_my_ip" {
  type            = "ingress"
  from_port       = 22
  to_port         = 22
  protocol        = "tcp"
  cidr_blocks     = "${var.ips_allowed}"

  security_group_id = "${aws_security_group.emr_master.id}"
}
