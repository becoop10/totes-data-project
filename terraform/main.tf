terraform {
  backend "s3" {
    bucket = "totes-amazeballs-s3-terraform-state-bucket"
    key = "terraform.tfstate"
    region = "us-east-1"

    # both access and secret keys will need to be changed for github secrets once that is working
    access_key = aws_iam_access_key.terraform_state_access_key.id 
    secret_key = aws_iam_access_key.terraform_state_access_key.secret
  }
}