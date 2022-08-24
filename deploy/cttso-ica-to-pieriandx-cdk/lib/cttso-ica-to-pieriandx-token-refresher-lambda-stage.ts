import {CfnOutput, StackProps, Stage, Tags} from "aws-cdk-lib";
import { Construct } from "constructs";
import { CttsoIcaToPieriandxTokenRefreshLambdaStack } from "./cttso-ica-to-pieriandx-token-refresher-lambda-stack"


interface CttsoIcaToPieriandxTokenRefreshLambdaStageProps extends StackProps {
    stack_prefix: string
    env: {
        account: string
        region: string
    }
    stack_suffix: string
}

export class CttsoIcaToPieriandxTokenRefreshLambdaStage extends Stage {

    public readonly tokenRefreshLambdaFunctionArnOutput: CfnOutput
    public readonly tokenRefreshLambdaFunctionSSMParameterOutput: CfnOutput

    constructor(
        scope: Construct,
        id: string,
        props: CttsoIcaToPieriandxTokenRefreshLambdaStageProps
    ) {
        super(scope, id, props);

        const lambda_batch_stack = new CttsoIcaToPieriandxTokenRefreshLambdaStack(this, `${props.stack_prefix}-tokenRefresh-lambda-stack`, props);

        Tags.of(lambda_batch_stack).add(`${props.stack_prefix}-token-refresh-lambda-stack`, props.stack_prefix);

        this.tokenRefreshLambdaFunctionArnOutput = lambda_batch_stack.tokenRefreshLambdaFunctionArnOutput
        this.tokenRefreshLambdaFunctionSSMParameterOutput = lambda_batch_stack.tokenRefreshLambdaFunctionSSMParameterOutput
    }
}
