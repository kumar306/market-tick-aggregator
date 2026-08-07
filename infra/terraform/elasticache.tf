# creation of the elasticache subnet group, sg and the elasticache instance
resource "aws_elasticache_subnet_group" "redis" {
  name = "mta-redis-subnets"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_security_group" "redis" {
  name_prefix = "mta-redis-"
  vpc_id = module.vpc.vpc_id

  ingress {
    description = "Redis from EKS nodes"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [ module.eks.node_security_group_id ]
  }

  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = [ "0.0.0.0/0" ]
  }

   tags = {
    Name = "mta-redis"
  }
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id = "mta-redis"
  engine = "redis"
  engine_version  = "7.1"
#    2 vcpu, 0.5 GiB ram
  node_type = "cache.t4g.micro"
  num_cache_nodes = 1
  port = 6379

  subnet_group_name = aws_elasticache_subnet_group.redis.name
  security_group_ids = [ aws_security_group.redis.id ]
}

output "redis_endpoint" {
  value = aws_elasticache_cluster.redis.cache_nodes[0].address
}