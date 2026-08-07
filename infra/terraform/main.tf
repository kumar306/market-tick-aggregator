terraform {
  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "~> 5.0"
    }
    # command which i run to push images to ECR
    null = {
        source = "hashicorp/null"
        version = "~> 3.2"
    }

    # db password generation
    random = {
        source  = "hashicorp/random"
        version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}


# terraform destroy -target=module.eks -target=module.vpc -target=aws_msk_serverless_cluster.this -target=aws_security_group.msk_sg