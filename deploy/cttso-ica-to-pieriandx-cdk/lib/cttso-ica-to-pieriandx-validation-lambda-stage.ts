import {CfnOutput, StackProps, Stage, Tags} from "aws-cdk-lib";
import { Construct } from "constructs";
import { CttsoIcaToPieriandxValidationLambdaStack } from "./cttso-ica-to-pieriandx-validation-lambda-stack"


interface CttsoIcaToPieriandxValidationLambdaStageProps extends StackProps {
    stack_prefix: string
    env: {
        account: string
        region: string
    }
    stack_suffix: string
}

export class CttsoIcaToPieriandxValidationLambdaStage extends Stage {

    public readonly validationLambdaFunctionArnOutput: CfnOutput
    public readonly validationLambdaFunctionSSMParameterOutput: CfnOutput

    constructor(
        scope: Construct,
        id: string,
        props: CttsoIcaToPieriandxValidationLambdaStageProps
    ) {
        super(scope, id, props);

        const lambda_batch_stack = new CttsoIcaToPieriandxValidationLambdaStack(this, `${props.stack_prefix}-validation-lambda-stack`, props);

        Tags.of(lambda_batch_stack).add(`${props.stack_prefix}-validation-lambda-stack`, props.stack_prefix);

        this.validationLambdaFunctionArnOutput = lambda_batch_stack.validationLambdaFunctionArnOutput
        this.validationLambdaFunctionSSMParameterOutput = lambda_batch_stack.validationLambdaFunctionSSMParameterOutput
    }
}
