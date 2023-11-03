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

import { BatchWriteCommandInput, BatchWriteCommandOutput, DynamoDBDocument } from "@aws-sdk/lib-dynamodb";
import { DocumentClient, DynamoDB } from "@aws-sdk/client-dynamodb";

import { DynamoDbUtils } from './dynamoDb.util';

describe('DynamoDbUtils', () => {
    let mockedDocumentClient: DocumentClient;
    let instance: DynamoDbUtils;

    beforeEach(() => {
        mockedDocumentClient = DynamoDBDocument.from(new DynamoDB());
        const mockedDocumentClientFactory = () => {
            return mockedDocumentClient;
        };
        instance = new DynamoDbUtils(mockedDocumentClientFactory);
    });

    it('a batch is split into chunks', () => {
        // stubs
        const batch: BatchWriteCommandInput = {
            RequestItems: {
                table1: [
                    { PutRequest: { Item: { seq: { S: '1' } } } },
                    { PutRequest: { Item: { seq: { S: '2' } } } },
                ],
                table2: [
                    { PutRequest: { Item: { seq: { S: '3' } } } },
                    { PutRequest: { Item: { seq: { S: '4' } } } },
                    { PutRequest: { Item: { seq: { S: '5' } } } },
                ],
            },
        };

        const expected: BatchWriteCommandInput[] = [
            {
                RequestItems: {
                    table1: [
                        { PutRequest: { Item: { seq: { S: '1' } } } },
                        { PutRequest: { Item: { seq: { S: '2' } } } },
                    ],
                    table2: [{ PutRequest: { Item: { seq: { S: '3' } } } }],
                },
            },
            {
                RequestItems: {
                    table2: [
                        { PutRequest: { Item: { seq: { S: '4' } } } },
                        { PutRequest: { Item: { seq: { S: '5' } } } },
                    ],
                },
            },
        ];

        // execute
        const actual = instance.test___splitBatchWriteIntoChunks(batch, 3);

        // verify
        expect(actual).toBeDefined();
        expect(actual).toEqual(expected);
    });

    it('unprocessed chunks are rejoined', () => {
        // stubs
        const unprocessed: BatchWriteCommandOutput = {
            UnprocessedItems: {
                table1: [
                    { PutRequest: { Item: { seq: { S: '1' } } } },
                    { PutRequest: { Item: { seq: { S: '2' } } } },
                ],
                table2: [{ PutRequest: { Item: { seq: { S: '3' } } } }],
            },
        };
        const remaining: BatchWriteCommandInput[] = [
            {
                RequestItems: {
                    table2: [{ PutRequest: { Item: { seq: { S: '4' } } } }],
                    table3: [
                        { PutRequest: { Item: { seq: { S: '5' } } } },
                        { PutRequest: { Item: { seq: { S: '6' } } } },
                    ],
                },
            },
            {
                RequestItems: {
                    table4: [
                        { PutRequest: { Item: { seq: { S: '7' } } } },
                        { PutRequest: { Item: { seq: { S: '8' } } } },
                    ],
                },
            },
        ];

        const expected: BatchWriteCommandOutput = {
            UnprocessedItems: {
                table1: [
                    { PutRequest: { Item: { seq: { S: '1' } } } },
                    { PutRequest: { Item: { seq: { S: '2' } } } },
                ],
                table2: [
                    { PutRequest: { Item: { seq: { S: '3' } } } },
                    { PutRequest: { Item: { seq: { S: '4' } } } },
                ],
                table3: [
                    { PutRequest: { Item: { seq: { S: '5' } } } },
                    { PutRequest: { Item: { seq: { S: '6' } } } },
                ],
                table4: [
                    { PutRequest: { Item: { seq: { S: '7' } } } },
                    { PutRequest: { Item: { seq: { S: '8' } } } },
                ],
            },
        };

        // execute
        const actual = instance.test___joinChunksIntoOutputBatchWrite(unprocessed, remaining);

        // verify
        expect(actual).toBeDefined();
        expect(actual).toEqual(expected);
    });
});
