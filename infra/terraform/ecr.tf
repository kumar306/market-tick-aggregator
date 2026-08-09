# get the iam account to lookup its ecr url. create ecr repository per image
# login to ecr with local docker daemon - get my auth token via iam (12 hour temp)
# local docker daemon send a login request to ECR with AWS username, token to communicate with it
# ecr returns a bearer token used to push image with by local daemon - store bearer token in ~/docker/config.json
# i used a null resource in terraform for executing bash script as part of step as terraform doesnt have such a resource to do this

data "aws_caller_identity" "current" {}

locals {
    ecr_registry = "${data.aws_caller_identity.current.account_id}.dkr.ecr.us-east-1.amazonaws.com"

    services = {
        adapter     = "adapter/Dockerfile"
        aggregator  = "aggregator/Dockerfile"
        normalizer  = "normalizer/Dockerfile"
        orderbook   = "orderbook/Dockerfile"
        persistence = "persistence/Dockerfile"
        ui          = "ui/Dockerfile"
        ui-backend  = "ui-backend/Dockerfile"
        loadtest    = "loadtest/Dockerfile"
        mockexchange = "mockexchange/Dockerfile"
    }
}

resource "aws_ecr_repository" "this" {
    for_each = local.services
    name = "market-tick-${each.key}"
    # ensure no one can repush to a already pushed image causing image drift
    image_tag_mutability = "IMMUTABLE"

    image_scanning_configuration {
      scan_on_push = true
    }
}

resource "null_resource" "ecr_login" {
    # triggers allows step when any of the inside value is different , here timestamp() always different across runs
    triggers = {
      always_run = timestamp()
    }
 
    provisioner "local-exec" {
      interpreter = [ "bash", "-c" ]
      command = "aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${local.ecr_registry}"
    }
}

resource "null_resource" "push_image" {
    for_each = local.services

    triggers = {
      image_tag = var.image_tag
      repo_url = aws_ecr_repository.this[each.key].repository_url
    }

    depends_on = [ null_resource.ecr_login ]

    provisioner "local-exec" {
      interpreter = [ "bash", "-c" ]
      working_dir = "${path.module}/../.."
      command = <<-EOT
      docker build -f ${each.value} -t market-tick-${each.key}:${var.image_tag} .
      docker tag market-tick-${each.key}:${var.image_tag} ${aws_ecr_repository.this[each.key].repository_url}:${var.image_tag}
      docker push ${aws_ecr_repository.this[each.key].repository_url}:${var.image_tag}
      EOT
    }
}

output "ecr_repository_urls" {
  value = { for k, v in aws_ecr_repository.this : k => v.repository_url }
}