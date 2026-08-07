# get the arns for components that manage all topics, all consumer groups, transaction processign
locals {
    msk_topic_arn = "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":topic/" )}/*"
    msk_group_arn = "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":group/" )}/*"
    msk_transactional_id_arn = "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":transactional-id/")}/*"
}

# define the iam policy - msk permissions to connect, allow creation of topics, read, write, 
# alter consumer groups, transactional processing component
resource "aws_iam_policy" "msk_client" {
  name = "mta-msk-client-access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            Sid = "Connect"
            Effect = "Allow"
            Action   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
            Resource = aws_msk_serverless_cluster.this.arn
        },
        {
            Sid = "TopicAccess"
            Effect = "Allow"
            Action = [
                "kafka-cluster:CreateTopic",
                "kafka-cluster:DescribeTopic",
                "kafka-cluster:WriteData",
                "kafka-cluster:ReadData"
            ]
            Resource = local.msk_topic_arn
        },
        {
            Sid = "GroupAccess"
            Effect = "Allow"
            Action = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
            Resource = local.msk_group_arn
        },
        {
            Sid      = "TransactionalAccess"
            Effect   = "Allow"
            Action   = ["kafka-cluster:AlterTransactionalId", "kafka-cluster:DescribeTransactionalId"]
            Resource = local.msk_transactional_id_arn
        },
    ]
  })
}

# irsa - define who can use the iam policy - service account to get annotated with iam policy
# oidc to trust eks cluster
data "aws_iam_policy_document" "msk_irsa_trust" {
    statement {
      effect = "Allow"
      # so the token can be exchanged with sts and sts validates irsa trust doc
      actions = ["sts:AssumeRoleWithWebIdentity"]

      # external token provider - oidc provider  
      principals {
        type = "Federated"
        identifiers = [ module.eks.oidc_provider_arn ]
      }

    #   in the oidc issued token, 
    #   service account name should be kafka client else sts check fails
    #   receipient should be sts.amazonaws.com else sts check fails
      condition {
        test = "StringEquals"
        variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:sub"
        values = [ "system:serviceaccount:default:kafka-client" ]
      }

      condition {
        test     = "StringEquals"
        variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:aud"
        values   = ["sts.amazonaws.com"]
      }
    }
}

# create the iam role and link it to msk iam policy, sts check policy for assume role
resource "aws_iam_role" "msk_irsa" {
    name = "mta-msk-irsa"
    assume_role_policy = data.aws_iam_policy_document.msk_irsa_trust.json
}

resource "aws_iam_role_policy_attachment" "msk_irsa" {
    role = aws_iam_role.msk_irsa.name
    policy_arn = aws_iam_policy.msk_client.arn
}

output "msk_irsa_role_arn" {
    value = aws_iam_role.msk_irsa.arn
}