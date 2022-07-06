import {CfnOutput, StackProps, Stage, StageProps, Tags} from "aws-cdk-lib";
import { Construct } from "constructs";
import { CttsoIcaToPieriandxRedcapLambdaStack } from "./cttso-ica-to-pieriandx-redcap-lambda-stack"
import { IBucket } from "aws-cdk-lib/aws-s3";


interface CttsoIcaToPieriandxRedcapLambdaStageProps extends StackProps {
    stack_prefix: string
    env: {
        account: string
        region: string
    }
    stack_suffix: string
}

export class CttsoIcaToPieriandxRedcapLambdaStage extends Stage {

    public readonly redcapLambdaFunctionArnOutput: CfnOutput
    public readonly redcapLambdaFunctionSSMParameterOutput: CfnOutput

    constructor(
        scope: Construct,
        id: string,
        props: CttsoIcaToPieriandxRedcapLambdaStageProps
    ) {
        super(scope, id, props);

        const lambda_batch_stack = new CttsoIcaToPieriandxRedcapLambdaStack(this, `${props.stack_prefix}-redcap-lambda-stack`, props);

        Tags.of(lambda_batch_stack).add(`${props.stack_prefix}-redcap-lambda-stack`, props.stack_prefix);

        this.redcapLambdaFunctionArnOutput = lambda_batch_stack.lambdaFunctionArnOutput
        this.redcapLambdaFunctionSSMParameterOutput = lambda_batch_stack.lambdaFunctionSSMParameterOutput
    }
}
