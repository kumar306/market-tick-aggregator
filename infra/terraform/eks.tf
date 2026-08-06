module "eks" {
    source = "terraform-aws-modules/eks/aws"
    version = "~> 20.0"

    cluster_name = var.cluster_name
    cluster_version = "1.36"

    vpc_id = module.vpc.vpc_id
    subnet_ids = module.vpc.private_subnets

    # control plane
    cluster_endpoint_public_access = true

    enable_cluster_creator_admin_permissions = true

    # node IAM policies - AmazonEKSWorkerNodePolicy, AmazonEKS_CNI_Policy, and AmazonEC2ContainerRegistryReadOnly
    # control to eks control plane to run pods via eks, to scale up pods - assign secondary ENIs, IPs, pull ECR images after auth
    eks_managed_node_groups = {
        default = {
            instance_type = ["t3.medium"]
            min_size = 2
            max_size = 3
            desired_size = 2
        }
    }
}