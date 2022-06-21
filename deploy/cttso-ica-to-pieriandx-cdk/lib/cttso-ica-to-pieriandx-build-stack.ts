import {CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {Repository} from "aws-cdk-lib/aws-ecr";
import {
    REPO_NAME,
    ECR_REPOSITORY_NAME
} from "../constants";
import {BuildSpec, LinuxBuildImage, Project, Source} from "aws-cdk-lib/aws-codebuild";
import {ManagedPolicy, Role, ServicePrincipal} from "aws-cdk-lib/aws-iam";


interface CttsoIcaToPieriandxCodeBuildStackProps extends StackProps {
    stack_prefix: string
    env: {
        account: string
        region: string
    }
    github_branch_name: string
}

export class CttsoIcaToPieriandxCodeBuildStack extends Stack {

    public readonly codeBuildProjectOutputArn: CfnOutput;
    public readonly containerUri: string

    constructor(scope: Construct, id: string, props: CttsoIcaToPieriandxCodeBuildStackProps) {

        super(scope, id, props);

        // Pull out env parameters from property
        const env = props.env

        // Specify the ecr repository to create
        const ecr_repo = new Repository(
            this,
            props.stack_prefix + "ECR",
            {
                repositoryName: ECR_REPOSITORY_NAME,
                removalPolicy: RemovalPolicy.DESTROY
                // https://github.com/aws/aws-cdk/issues/12618
                // Hopefully an autodelete option will come soon.
            }
        )

        // Specify container image name
        const container_repo: string = `${env.account}.dkr.ecr.${env.region}.amazonaws.com`
        const container_name: string = ecr_repo.repositoryName
        const container_uri: string = `${container_repo}/${container_name}:latest`

        // BuildSpec Object (basically the same as the buildspec.yml file)
        const codebuild_buildspec = BuildSpec.fromObject(
            {
                version: "0.2",
                env: {
                    "variables": {
                        "CONTAINER_REPO": container_repo,
                        "CONTAINER_NAME": container_name,
                        "REGION": env.region
                    }
                },
                phases: {
                    "install": {
                        "runtime-versions": {
                            "python": "3.9"
                        }
                    },
                    "pre_build": {
                        "commands": [
                            // Set DEBIAN_FRONTEND env var to noninteractive
                            "export DEBIAN_FRONTEND=noninteractive",
                            // Update
                            "apt-get update -y -qq",
                            // Install git, unzip and wget
                            "apt-get install -y -qq git unzip wget",
                            // Install aws v2
                            "wget --quiet " +
                            "  --output-document \"awscliv2.zip\" " +
                            "  \"https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip\"",
                            "unzip -qq \"awscliv2.zip\"",
                            "./aws/install --update",
                            // Clean up
                            "rm -rf \"awscliv2.zip\" \"aws/\""
                        ]
                    },
                    "build": {
                        "commands": [
                            // Convenience CODEBUILD VARS, need more? Check https://github.com/thii/aws-codebuild-extras
                            "export CTTSO_ICA_TO_PIERIANDX_GIT_TAG=\"$(git describe --tags --exact-match 2>/dev/null)\"",
                            // Build as latest tag
                            "docker build --tag \"${CONTAINER_REPO}/${CONTAINER_NAME}:latest\" ./",
                            // Also add in tag if applicable
                            "if [ -n \"${CTTSO_ICA_TO_PIERIANDX_GIT_TAG-}\" ]; then " +
                            "  docker tag \"${CONTAINER_REPO}/${CONTAINER_NAME}:latest\" \"${CONTAINER_REPO}/${CONTAINER_NAME}:${CTTSO_ICA_TO_PIERIANDX_GIT_TAG}\"; " +
                            "fi",
                            "export \"CTTSO_ICA_TO_PIERIANDX_GIT_TAG\"",
                            "echo \"Container version is ${CTTSO_ICA_TO_PIERIANDX_GIT_TAG-latest}\" 1>&2"
                        ]
                    },
                    "post_build": {
                        "commands": [
                            // Login to aws and push Docker image to ECR
                            "aws ecr get-login-password --region \"${REGION}\" | docker login --username AWS --password-stdin \"${CONTAINER_REPO}\"",
                            "docker push \"${CONTAINER_REPO}/${CONTAINER_NAME}:latest\"",
                            "if [ -n \"${CTTSO_ICA_TO_PIERIANDX_GIT_TAG-}\" ]; then  " +
                            "  docker push \"${CONTAINER_REPO}/${CONTAINER_NAME}:${CTTSO_ICA_TO_PIERIANDX_GIT_TAG}\";  " +
                            "fi"
                        ]
                    }
                }
            }
        )

        // Set codebuild source
        const github_source = Source.gitHub({
            owner: REPO_NAME.split("/").splice(0, 1)[0],
            repo: REPO_NAME.split("/").splice(-1, 1)[0],
            branchOrRef: props.github_branch_name
        })

        // Set up role for codebuild
        const codebuild_role = new Role(
            this,
            `${props.stack_prefix}-codebuild-role`,
            {
                assumedBy: new ServicePrincipal("codebuild.amazonaws.com"),
                managedPolicies: [
                    ManagedPolicy.fromAwsManagedPolicyName("AmazonEC2ContainerRegistryFullAccess ")
                ]
            }
        )

        // Create codebuild from object
        const codebuild_obj = new Project(
            this,
            props.stack_prefix + "cb-project",
            {
                buildSpec: codebuild_buildspec,
                source: github_source,
                environment: {
                    buildImage: LinuxBuildImage.STANDARD_5_0,
                    privileged: true
                },
                timeout: Duration.hours(3),
                // Need to be able to push to ecr
                role: codebuild_role
            }
        )

        // Add codebuild arn as output
        this.codeBuildProjectOutputArn = new CfnOutput(this, "codeBuildProjectOutputArn", {
            value: codebuild_obj.projectArn,
        });

        // Add image tag as output
        this.containerUri = container_uri
    }
}
