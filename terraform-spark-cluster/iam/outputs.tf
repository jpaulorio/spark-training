output "emr_ec2_instance_profile_arn" {
  value = "${aws_iam_instance_profile.emr_ec2_instance_profile.arn}"
}

output "emr_service_role_arn" {
  value = "${aws_iam_role.emr_service_role.arn}"
}
