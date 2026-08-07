# creation of the rds subnet group, sg and the rds instance
resource "aws_db_subnet_group" "postgres" {
    name = "mta-postgres-subnets"
    subnet_ids = module.vpc.private_subnets
}

resource "aws_security_group" "postgres" {
    name_prefix = "mta-postgres-"
    vpc_id = module.vpc.vpc_id

    ingress {
        description = "Postgres from EKS nodes"
        from_port = 5432
        to_port = 5432
        protocol = "tcp"
        security_groups = [module.eks.node_security_group_id]
    }

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }

    tags = {
        Name = "mta-postgres"
    }
}

resource "random_password" "postgres" {
  length = 24
  special = false
}

resource "aws_db_instance" "postgres" {
  identifier = "mta-postgres"
  engine = "postgres"
  engine_version = "16"
#   2vcpu, 1 GiB
  instance_class = "db.t4g.micro"

    # 20 gb storage, max 125 MB/s throughput on gp3
  allocated_storage = 20
  storage_type = "gp3"

  db_name = "markettick"
  username = "markettick_admin"
  password = random_password.postgres.result

  db_subnet_group_name = aws_db_subnet_group.postgres.name
  vpc_security_group_ids = [ aws_security_group.postgres.id ]

  publicly_accessible = false
  multi_az            = false

  backup_retention_period = 0
  skip_final_snapshot     = true
  deletion_protection     = false
}

output "postgres_endpoint" {
    value = aws_db_instance.postgres.endpoint
}

output "postgres_password" {
    value = random_password.postgres.result
    sensitive = true
}