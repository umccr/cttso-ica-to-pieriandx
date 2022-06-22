import {CfnOutput, StackProps, Stage, StageProps, Tags} from "aws-cdk-lib";
import { Construct } from "constructs";
import { CttsoIcaToPieriandxBatchStack } from "./cttso-ica-to-pieriandx-batch-stack"
import { IBucket } from "aws-cdk-lib/aws-s3";


interface CttsoIcaToPieriandxBatchStageProps extends StackProps {
    stack_prefix: string
    env: {
        account: string
        region: string
    }
}

export class CttsoIcaToPieriandxBatchStage extends Stage {

    public readonly BatchJobDefinitionArn: CfnOutput
    public readonly LambdaFunctionArn: CfnOutput

    constructor(
        scope: Construct,
        id: string,
        props: CttsoIcaToPieriandxBatchStageProps
    ) {
        super(scope, id, props);

        const lambda_batch_stack = new CttsoIcaToPieriandxBatchStack(this, props.stack_prefix, props);

        Tags.of(lambda_batch_stack).add(`${props.stack_prefix}-Stack`, props.stack_prefix);

        this.BatchJobDefinitionArn = lambda_batch_stack.BatchJobDefinitionArn
        this.LambdaFunctionArn = lambda_batch_stack.LambdaFunctionArn
    }
}
