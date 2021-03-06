#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { CttsoIcaToPieriandxPipelineStack } from '../lib/cttso-ica-to-pieriandx-pipeline-stack';
import {
    AWS_BUILD_ACCOUNT_ID,
    AWS_DEV_ACCOUNT_ID,
    AWS_PROD_ACCOUNT_ID,
    AWS_REGION,
    DEV_STACK_SUFFIX,
    PROD_STACK_SUFFIX,
    GITHUB_DEV_BRANCH_NAME,
    GITHUB_PROD_BRANCH_NAME,
    STACK_PREFIX
} from "../constants";

const app = new cdk.App();

// Development stack
const dev_stack = new CttsoIcaToPieriandxPipelineStack(app, 'CttsoIcaToPieriandxPipelineStackDev', {
    stack_prefix: `${STACK_PREFIX}-${DEV_STACK_SUFFIX}`,
    aws_region: AWS_REGION,
    aws_account_id: AWS_DEV_ACCOUNT_ID,
    github_branch_name: GITHUB_DEV_BRANCH_NAME,
    env: {
        account: AWS_BUILD_ACCOUNT_ID,
        region: AWS_REGION
    },
    stack_suffix: DEV_STACK_SUFFIX
});

const prod_stack = new CttsoIcaToPieriandxPipelineStack(app, 'CttsoIcaToPieriandxPipelineStackProd', {
    stack_prefix: `${STACK_PREFIX}-${PROD_STACK_SUFFIX}`,
    aws_region: AWS_REGION,
    aws_account_id: AWS_PROD_ACCOUNT_ID,
    github_branch_name: GITHUB_PROD_BRANCH_NAME,
    env: {
        account: AWS_BUILD_ACCOUNT_ID,
        region: AWS_REGION
    },
    stack_suffix: PROD_STACK_SUFFIX
});
