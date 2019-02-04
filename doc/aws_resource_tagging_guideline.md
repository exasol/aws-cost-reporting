# AWS Resource Tagging Guideline

## Situation

It is quite easy to incur a five digit monthly cost on AWS. High costs as a total are an issue already, but it is even worse if we can't tell where they come from and whether or not they are justified by the use case.

Additionally cloud infrastructure tends to grow out of control if it is not regularly cleaned up.


## Goals

1. Each project gets billed for the AWS costs it incurred
1. We keep our AWS setup clean (i.e. minimal)
1. There is an owner for each resource who can decide whether or not it can be removed
1. There is an owner for each resource who holds the final responsibility of the resources security

## Tagging and Billing

AWS supports allocation costs to an entity via tags. This concept is called [Cost Allocation Tags](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/cost-alloc-tags.html). Tags are key-value pairs, some of which AWS automatically creates and others that you need to add.

Once the tags are in place and cost allocation by tags is enabled, AWS produces a report that maps the incurred costs to those labels.


## Resource Tags

The following list contains that tags that we require for each AWS resource you create. Some of them are mandatory others optional.

Most, but not all AWS resources support tagging, obviously you only need to tag those that AWS supports.

| Tag | Mandatory | Content Format | Examples
---------------------------------------------
| Name             | y | Name of the resource displayed in overview lists. | This key is reserved by AWS and must be written exactly like this (i.e. with an upper case "N" in front). `<project short tag (uppercase)>-<resource name (lower kebab-case)>`  DRPOC-application-node |
| exa:owner        | y | Name of the person (no distribution list) responsible for this resource. | Company email address. All letters lower case: `jane.doe@example.com` |
| exa:deputy       | n | Name of a deputy for the responsible (distribution list possible). "Deputy" means the stand-in for the owner in case the owner is not reachable (e.g. in case of vacation or sick leave). |
Company email address. All letters lower case: `john.smith@example.com` |
| exa:project      | y | Project short tag.  Identical to JIRA short tag of the project. | Pattern: [A-Z0-9]+  XYZPOC |
| exa:project.name | n | Human readable project name. | Free text. Up to 256 UTF characters. | XYZ Proof-of-Concept |
| exa:department   | y | The department that owns the resource   one of: "RnD", "PM", "PreSales", "ITS", "Support" | PreSales |
| exa:stage        | n | The stage the resources in a stack belong to. | One of:
* "development" tests during development
* (often one stack per developer)
* "integration test"
* "system test"
* "demo" (e.g. for pre-sales demonstrators)
* "pre-live"
* "live"
| exa:customer     | n | The customer we do the project / PoC / demo / product extension for. | Official company name of the customer. Please try to look up the official name including the correct case: "Acme Corp." |
| exa:issue        | n | In case the JIRA project does not distinguish the purpose of the resource well enough, you can relate the resource to a JIRA issue additionally. | JIRA ticket ID: `FOOBAR-12345` |

Note that both keys and values are case sensitive, so please adhere to the formats specified above strictly.

## What to Tag?

Short answer: everything where AWS supports a tag.

Long answer:

Pretty much everything you use on AWS contributes to the bill, so the best solution is to simply tag everything. Note that this is also relevant for resources that AWS creates automatically.

For example you can let AWS [tag EC2 instances in an auto-scaling group](https://docs.aws.amazon.com/autoscaling/ec2/userguide/autoscaling-tagging.html).

Things you need to tag include but are not limited to:

* EC2 launch templates
* EC2 instances
* EBS volumes
* VPCs
* Subnets
* Security Groups
* S3 buckets
* DynamoDB
* etc.

You can find a complete list of EC2 related resources that support tags here: [Tagging Support for Amazon EC2 Resources](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html#tag-resources). Most other services support tags too.

Note that of the tags mentioned above only Name and `exa:project` are set here, because we used Resource Groups to add the remaining tags. This saves a lot of effort an is less error prone.

### Using Resource Groups

You can save a lot of effort if you use resource groups for tagging the resources.

#### Variant A: Tag-based Resource Groups

The simplest way is to add only the `exa:project` label to the resource itself and let AWS associate all other labels automatically by creating a resource group where the group members are identified by that tag. If not all tag values are identified by the project alone, you have to define the grouping criteria with more than just one tag.

For example it could be that in the same project different people one the live and demo stage.

#### Variant B: Stack-based Resource Groups

If the resources were created by [CloudFormation](https://aws.amazon.com/cloudformation/) you can alternatively use the stack itself as grouping criteria.

### Group Details and Tags

AWS associates all resources that match the filter criteria with that group.

The rest of the group configuration is a name, a description and the tags that are associated with the resources via the group rather than individually.

Resource groups are [region-based](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions) (eg. `eu-central-1` or `us-east-1`). That means

* the resource group must be defined in the same region as the resources it is supposed to group
* if you distribute your project across multiple regions, you need to create a resource group in each of them

## What Happens if I Forget Mandatory Tags?

In case the owner and or deputy are set correctly, an email will be sent to the owner and deputy naming the resource that needs to be tagged. They have two weeks time to comply with the request after which the resource in question will be automatically deleted.

## Resource Names Which are no Tags

In AWS the display name strategy is not 100% uniform for all resources. In case where resources feature tagging, the display name is set by the reserved Name tag.

Unlike tag regular resource names cannot be used for separating bills. They are very useful nonetheless when looking at long lists of resources.

Please prefix all your project-related resources with the project short tag (like in JIRA).

Format:

    <project short tag (uppercase)>-<resource name (lower kebab-case)>

Examples:

* `XYZPOC-compute-node`
* `ABCETL-etl-source-bucket`

Please name your resources accordingly.

Prefixes are most important for resources that do not support tagging (e.g. key pairs).

## Delete Volumes on Instance Termination

One way to simplify the AWS clean-up is to make sure that EBS volumes are deleted when the corresponding instance is terminated. Unless you plan to change the instance type someday, this is always the preferred standard option. Even if you did not select it in the beginning, you can still migrate volumes by making snapshots