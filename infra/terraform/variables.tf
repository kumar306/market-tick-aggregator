variable "cluster_name" {
    description = "EKS cluster used in VPC subnets"
    type = string
    default = "mta-eks"
}

variable "image_tag" {
   description = "Tag applied to each pushed image. Increase version after each push"
   type = string
   default = "v1"
}