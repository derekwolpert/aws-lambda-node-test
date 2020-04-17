const AWS = require("aws-sdk");
const sourceDataset = require("./source_data").sourceDataset;

process.env.AWS_DATA_PATH = "/opt/";

const dataexchange = new AWS.DataExchange({
    region: process.env.REGION
});

const marketplace = new AWS.MarketplaceCatalog({
    region: process.env.REGION
});

const s3Bucket = process.env.S3_BUCKET;
const dataSetArn = process.env.DATA_SET_ARN;
const dataSetId = dataSetArn.slice(dataSetArn.indexOf("/") + 1);
const productId = process.env.PRODUCT_ID;
const dataSetName = process.env.DATA_SET_NAME;
const newS3Key = dataSetName + "/dataset/";
// const cfnTemplate = dataSetName + "/automation/cloudformation.yaml";
// const postProcessingCode =
//     dataSetName + "/automation/post-processing-code.zip";

const today = new Date();
const revisionComment =
    "Revision Updates v" + today.toISOString().slice(0, 10);

if (!s3Bucket) {
    throw "'S3_BUCKET' environment variable must be defined!";
}

if (!newS3Key) {
    throw "'DATA_SET_NAME' environment variable must be defined!";
}

if (!dataSetArn) {
    throw "'DATA_SET_ARN' environment variable must be defined!";
}

if (!productId) {
    throw "'PRODUCT_ID' environment variable must be defined!";
}

const startChangeSet = (describeEntityResponse, revisionArn) => {

    const changeDetails = {
        DataSetArn: dataSetArn,
        RevisionArns: [revisionArn],
    };

    const changeSet = [
        {
            ChangeType: "AddRevisions",
            Entity: {
                Type: describeEntityResponse.EntityType,
                Identifier: describeEntityResponse.EntityIdentifier
            },
            Details: JSON.stringify(changeDetails),
        },
    ];

    const params = {
        Catalog: "AWSMarketplace",
        changeSet
    };

    const response = marketplace.startChangeSet(params, (err, data) => {
        if (err) {
            throw err;
        } else {
            return data;
        }
    });
    
    return response;
};

exports.lambdaHandler = (event, context) => {
    
    sourceDataset(s3Bucket, newS3Key);

    const createRevisionResponse = dataexchange.createRevision(
        { DataSetId: dataSetId },
        (err , data) => {
            if (err) {
                throw err;
            } else {
                return data;
            }
        }
    );

    const [revisionId, revisionArn] = [
        createRevisionResponse.Id,
        createRevisionResponse.Arn,
    ];

    const jobIds = new Set();

    const importJobParams = {
        Type: "IMPORT_ASSETS_FROM_S3",
        Details: {
            ImportAssetsFromS3: {
                DataSetId: dataSetId,
                RevisionId: revisionId,
                AssetSources: [
                    {
                        Bucket: s3Bucket,
                        Key: newS3Key + "us-states.csv",
                    },
                    {
                        Bucket: s3Bucket,
                        Key: newS3Key + "us-counties.csv",
                    },
                ],
            },
        },
    };

    const importJob = dataexchange.createJob(importJobParams, (err, data) => {
        if (err) {
            throw err;
        } else {
            return data;
        }
    });

    dataexchange.startJob({ JobId: importJob.Id }, (err, data) => {
        if (err) {
            throw err;
        } else {
            return data;
        }
    });

    jobIds.add(importJob.Id);

    const completedJobs = new Set();

    let jobsCompleted = false;

    while (!jobsCompleted) {
        jobsCompleted = true;
        
        for (const jobId of jobIds) {
            if (!completedJobs.has(jobId)) {
                jobsCompleted = false;
                const getJobResponse = dataexchange.getJob(
                    { JobId: jobId },
                    (err, data) => {
                        if (err) {
                            throw err;
                        } else {
                            return data;
                        }
                    }
                );

                if (getJobResponse.State === "COMPLETED") {
                    console.log(`Job ${jobId} completed`);
                    completedJobs.add(jobId);
                }

                if (getJobResponse.State === "ERROR") {
                    const jobErrors = getJobResponse.Errors;
                    throw `JobId: ${jobId} failed with errors: ${jobErrors}`;
                }
            }
            sleep(200);
        }
    }

    const updateRevisionParams = {
        DataSetId: dataSetId,
        RevisionId: revisionId,
        Comment: revisionComment,
        Finalized: true,
    };

    const updateRevisionResponse = dataexchange.updateRevision(
        updateRevisionParams,
        (err, data) => {
            if (err) {
                throw err;
            } else {
                return data;
            }
        }
    );

    const revisionState = updateRevisionResponse.Finalized;

    if (revisionState === true) {

        const describeEntityParams = {
            Catalog: "AWSMarketplace",
            EntityId: productId
        };

        const describeEntityResponse = marketplace.describeEntity(
            describeEntityParams,
            (err, data) => {
                if (err) {
                    throw err;
                } else {
                    return data;
                }
            }
        );

        const startChangeSetResponse = startChangeSet(
            describeEntityResponse,
            revisionArn
        );

        if (startChangeSetResponse.ChangeSetId) {
            return {
                statusCode: 200,
                body: JSON.stringify(
                    "Revision updated successfully and added to the dataset"
                )
            };
        } else {
            return {
                statusCode: 500,
                body: JSON.stringify(
                    "Something went wrong with AWSMarketplace Catalog API"
                ),
            };
        }
    } else {
        return {
            statusCode: 500,
            body: JSON.stringify("Revision did not complete successfully"),
        };
    }
};