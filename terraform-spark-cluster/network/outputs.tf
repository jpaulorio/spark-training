output "vpc_id" {
  value = "${aws_vpc.spark_cluster.id}"
}

output "subnet_id" {
  value = "${aws_subnet.spark_cluster.id}"
}

output "sg_emr_master_id" {
  value = "${aws_security_group.emr_master.id}"
}

output "sg_emr_slave_id" {
  value = "${aws_security_group.emr_slave.id}"
}
