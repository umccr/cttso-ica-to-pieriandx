import {CfnOutput, StackProps, Stage, StageProps, Tags} from "aws-cdk-lib";
import { Construct } from "constructs";
import { CttsoIcaToPieriandxCodeBuildStack } from "./cttso-ica-to-pieriandx-build-stack"


interface CttsoIcaToPieriandxCodeBuildStageProps extends StackProps {
    stack_prefix: string
    env: {
        account: string
        region: string
    }
}

export class CttsoIcaToPieriandxCodeBuildStage extends Stage {

    public readonly codeBuildProjectOutputArn: CfnOutput
    public readonly containerUri: string

    constructor(
        scope: Construct,
        id: string,
        props: CttsoIcaToPieriandxCodeBuildStageProps
    ) {
        super(scope, id, props);

        const docker_build_stack = new CttsoIcaToPieriandxCodeBuildStack(this, props.stack_prefix, props);

        Tags.of(docker_build_stack).add(`${props.stack_prefix}-Stack`, props.stack_prefix);

        this.codeBuildProjectOutputArn = docker_build_stack.codeBuildProjectOutputArn
        this.containerUri = docker_build_stack.containerUri
    }
}
