terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "4.54.0"
    }
  }
  required_version = ">= 1.1.0"

  cloud {
    organization = "example-org-909be3"

    workspaces {
      name = "gh-actions"
    }
  }
}

provider "aws" {
    region = "us-east-1"
}