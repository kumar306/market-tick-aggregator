resource "aws_security_group" "msk_sg" {
  name_prefix = "mta-msk-"
  vpc_id = module.vpc.vpc_id

  # allow the nodes in private subnet to access this msk sg at 9098 
  # SASL/SSL which hits msk serverless via vpc endpoint
  ingress {
    description = "IAM authenticated Kafka from EKS nodes"
    from_port = 9098
    to_port = 9098
    protocol = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }

  # outbound traffic - anywhere, 0 includes all ports
  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_msk_serverless_cluster" "this" {
    cluster_name = "mta-msk-serverless"

    vpc_config {
      subnet_ids = module.vpc.private_subnets
      security_group_ids = [ aws_security_group.msk_sg.id ]
    }

    client_authentication {
      sasl {
        iam {
          enabled = true
        }
      }
    }
}

output "msk_bootstrap_brokers" {
  value = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam
}