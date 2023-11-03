/*********************************************************************************************************************
 *  Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.                                           *
 *                                                                                                                    *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance    *
 *  with the License. A copy of the License is located at                                                             *
 *                                                                                                                    *
 *      http://www.apache.org/licenses/LICENSE-2.0                                                                    *
 *                                                                                                                    *
 *  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES *
 *  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    *
 *  and limitations under the License.                                                                                *
 *********************************************************************************************************************/
import 'reflect-metadata';

import '@awssolutions/cdf-config-inject';

import { Container, decorate, injectable, interfaces } from 'inversify';

// Note: importing @controller's carries out a one time inversify metadata generation...
import '../things/bulkthings.controller';
import { AttachAdditionalPoliciesProcessor } from '../things/steps/attachAdditionalPoliciesProcessor';
import { ClientIdEnforcementPolicyStepProcessor } from '../things/steps/clientIdEnforcementPolicyStepProcessor';
import { CreateAwsCertiticateProcessor } from '../things/steps/createAwsCertificateProcessor';
import { CreateDeviceCertificateStepProcessor } from '../things/steps/createDeviceCertificateProcessor';
import { RegisterDeviceCertificateWithoutCAStepProcessor } from '../things/steps/registerDeviceCertificateWithoutCaProcessor';
import { UseACMPCAStepProcessor } from '../things/steps/useACMPCAProcessor';
import '../things/things.controller';
import { ThingsService } from '../things/things.service';
import { CertUtils } from '../utils/cert';
import { TYPES } from './types';

import AWS from 'aws-sdk';

import { ACMPCA, ClientConfiguration } from "@aws-sdk/client-acm-pca";
import { IoT } from "@aws-sdk/client-iot";
import { S3 } from "@aws-sdk/client-s3";
import { SNS } from "@aws-sdk/client-sns";
import { SSM } from "@aws-sdk/client-ssm";
import { STS } from "@aws-sdk/client-sts";

// Load everything needed to the Container
export const container = new Container();

// config

container.bind<string>('aws.s3.roleArn').toConstantValue(process.env.AWS_S3_ROLE_ARN);
container
    .bind<string>('aws.s3.templates.bucket')
    .toConstantValue(process.env.AWS_S3_TEMPLATES_BUCKET);
container
    .bind<string>('aws.s3.templates.prefix')
    .toConstantValue(process.env.AWS_S3_TEMPLATES_PREFIX);
container
    .bind<string>('aws.s3.templates.suffix')
    .toConstantValue(process.env.AWS_S3_TEMPLATES_SUFFIX);
container
    .bind<string>('aws.s3.bulkrequests.bucket')
    .toConstantValue(process.env.AWS_S3_BULKREQUESTS_BUCKET);
container
    .bind<string>('aws.s3.bulkrequests.prefix')
    .toConstantValue(process.env.AWS_S3_BULKREQUESTS_PREFIX);
container
    .bind<boolean>('features.delete.certificates')
    .toConstantValue(process.env.FEATURES_DELETE_CERTIFICATES === 'true');
container
    .bind<boolean>('features.delete.policies')
    .toConstantValue(process.env.FEATURES_DELETE_POLICIES === 'true');
container.bind<string>('aws.region').toConstantValue(process.env.AWS_REGION);
container.bind<string>('aws.accountId').toConstantValue(process.env.AWS_ACCOUNTID);
container
    .bind<string>('deviceCertificateExpiryDays')
    .toConstantValue(process.env.DEVICE_CERTIFICATE_EXPIRY_DAYS);

container.bind<ThingsService>(TYPES.ThingsService).to(ThingsService).inSingletonScope();
container.bind<CertUtils>(TYPES.CertUtils).to(CertUtils).inSingletonScope();

container
    .bind<ClientIdEnforcementPolicyStepProcessor>(TYPES.ClientIdEnforcementPolicyStepProcessor)
    .to(ClientIdEnforcementPolicyStepProcessor)
    .inSingletonScope();
container
    .bind<CreateDeviceCertificateStepProcessor>(TYPES.CreateDeviceCertificateStepProcessor)
    .to(CreateDeviceCertificateStepProcessor)
    .inSingletonScope();
container
    .bind<CreateAwsCertiticateProcessor>(TYPES.CreateAwsCertiticateProcessor)
    .to(CreateAwsCertiticateProcessor)
    .inSingletonScope();
container
    .bind<RegisterDeviceCertificateWithoutCAStepProcessor>(
        TYPES.RegisterDeviceCertificateWithoutCAStepProcessor
    )
    .to(RegisterDeviceCertificateWithoutCAStepProcessor)
    .inSingletonScope();
container
    .bind<UseACMPCAStepProcessor>(TYPES.UseACMPCAStepProcessor)
    .to(UseACMPCAStepProcessor)
    .inSingletonScope();
container
    .bind<AttachAdditionalPoliciesProcessor>(TYPES.AttachAdditionalPoliciesProcessor)
    .to(AttachAdditionalPoliciesProcessor)
    .inSingletonScope();

// for 3rd party objects, we need to use factory injectors
// IoT
decorate(injectable(), AWS.Iot);
container.bind<interfaces.Factory<AWS.Iot>>(TYPES.IotFactory).toFactory<AWS.Iot>(() => {
    return () => {
        if (!container.isBound(TYPES.Iot)) {
            const iot = new IoT({
                region: process.env.AWS_REGION
            });
            container.bind<AWS.Iot>(TYPES.Iot).toConstantValue(iot);
        }
        return container.get<AWS.Iot>(TYPES.Iot);
    };
});

// S3
decorate(injectable(), AWS.S3);
container.bind<interfaces.Factory<AWS.S3>>(TYPES.S3Factory).toFactory<AWS.S3>(() => {
    return () => {
        if (!container.isBound(TYPES.S3)) {
            const s3 = new S3({
                region: process.env.AWS_REGION
            });
            container.bind<AWS.S3>(TYPES.S3).toConstantValue(s3);
        }
        return container.get<AWS.S3>(TYPES.S3);
    };
});

// SSM
decorate(injectable(), AWS.SSM);
container.bind<interfaces.Factory<AWS.SSM>>(TYPES.SSMFactory).toFactory<AWS.SSM>(() => {
    return () => {
        if (!container.isBound(TYPES.SSM)) {
            const ssm = new SSM({
                region: process.env.AWS_REGION
            });
            container.bind<AWS.SSM>(TYPES.SSM).toConstantValue(ssm);
        }
        return container.get<AWS.SSM>(TYPES.SSM);
    };
});

// STS
decorate(injectable(), AWS.STS);
container.bind<interfaces.Factory<AWS.STS>>(TYPES.STSFactory).toFactory<AWS.STS>(() => {
    return () => {
        if (!container.isBound(TYPES.STS)) {
            const sts = new STS({
                region: process.env.AWS_REGION
            });
            container.bind<AWS.STS>(TYPES.STS).toConstantValue(sts);
        }
        return container.get<AWS.STS>(TYPES.STS);
    };
});

// SNS
decorate(injectable(), AWS.SNS);
container.bind<interfaces.Factory<AWS.SNS>>(TYPES.SNSFactory).toFactory<AWS.SNS>(() => {
    return () => {
        if (!container.isBound(TYPES.SNS)) {
            const sns = new SNS({
                region: process.env.AWS_REGION
            });
            container.bind<AWS.SNS>(TYPES.SNS).toConstantValue(sns);
        }
        return container.get<AWS.SNS>(TYPES.SNS);
    };
});

// ACMPCA
let acmPcaStsExpiresAt: Date;
decorate(injectable(), AWS.ACMPCA);
container.bind<interfaces.Factory<AWS.ACMPCA>>(TYPES.ACMPCAFactory).toFactory<AWS.ACMPCA>(() => {
    return () => {
        const region = process.env.ACM_REGION ?? process.env.AWS_REGION;

        // if not bound yet, or the STS token is expired, bind it
        if (!container.isBound(TYPES.ACMPCA) || isStsTokenAboutToExpire(acmPcaStsExpiresAt)) {
            const acmpcaConfig: ClientConfiguration = {
                region,
            };

            // ACM PCA may be held in a different account. If so, assume a role that allows access before creating the client
            const crossAccountRole = process.env.ACM_PCA_CROSS_ACCOUNT_ROLE_ARN;
            if (crossAccountRole) {
                // these credentials will be used for ACM PCA access
                const sts = container.get<AWS.STS>(TYPES.STS);
                sts.assumeRole(
                    {
                        RoleArn: crossAccountRole,
                        RoleSessionName: 'cdf-provisioning',
                    },
                    (err, data) => {
                        if (err) {
                            throw err;
                        } else {
                            acmpcaConfig.credentials.accessKeyId = data.Credentials.AccessKeyId;
                            acmpcaConfig.credentials.secretAccessKey =
                                data.Credentials.SecretAccessKey;
                            acmpcaConfig.credentials.sessionToken = data.Credentials.SessionToken;
                            acmPcaStsExpiresAt = data.Credentials.Expiration;
                        }
                    }
                );
            }

            const acmpca = new ACMPCA(acmpcaConfig);
            if (container.isBound(TYPES.ACMPCA)) {
                container.rebind<AWS.ACMPCA>(TYPES.ACMPCA).toConstantValue(acmpca);
            } else {
                container.bind<AWS.ACMPCA>(TYPES.ACMPCA).toConstantValue(acmpca);
            }
        }
        return container.get<AWS.ACMPCA>(TYPES.ACMPCA);
    };
});

async function isStsTokenAboutToExpire(expiresAt: Date) {
    if (expiresAt === undefined) {
        return false;
    }
    const refreshTokenAt = new Date(Date.now() - 5 * 60 * 1000);
    return refreshTokenAt < expiresAt;
}
