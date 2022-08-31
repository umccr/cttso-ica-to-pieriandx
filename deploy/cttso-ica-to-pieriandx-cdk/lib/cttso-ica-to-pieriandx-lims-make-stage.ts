import {CfnOutput, StackProps, Stage, Tags} from "aws-cdk-lib";
import { Construct } from "constructs";
import { CttsoIcaToPieriandxLimsMakerLambdaStack } from "./cttso-ica-to-pieriandx-lims-maker-lambda-stack"


interface CttsoIcaToPieriandxLimsLambdaStageProps extends StackProps {
    stack_prefix: string
    env: {
        account: string
        region: string
    }
    stack_suffix: string
}

export class CttsoIcaToPieriandxLimsMakerLambdaStage extends Stage {

    public readonly limsLambdaFunctionArnOutput: CfnOutput
    public readonly limsLambdaFunctionSSMParameterOutput: CfnOutput

    constructor(
        scope: Construct,
        id: string,
        props: CttsoIcaToPieriandxLimsLambdaStageProps
    ) {
        super(scope, id, props);

        const lambda_batch_stack = new CttsoIcaToPieriandxLimsMakerLambdaStack(this, props.stack_prefix, props);

        Tags.of(lambda_batch_stack).add("Stack", props.stack_prefix);

        this.limsLambdaFunctionArnOutput = lambda_batch_stack.lambdaFunctionArnOutput
        this.limsLambdaFunctionSSMParameterOutput = lambda_batch_stack.lambdaFunctionSSMParameterOutput
    }
}
