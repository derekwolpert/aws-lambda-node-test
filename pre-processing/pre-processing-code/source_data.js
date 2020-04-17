const AWS = require("aws-sdk");
const fs = require("fs");
const https = require("https");

exports.sourceDataset = (s3Bucket, newS3Key) => {
    const baseUrl =
        "https://raw.githubusercontent.com/nytimes/covid-19-data/master/";

    const path = "/tmp";

    const s3 = new AWS.S3();

    const usStatesTitle = "us-states.csv";
    const usStatesUrl = baseUrl + usStatesTitle;
    const usStatesCsv = fs.createWriteStream(path + "/" + usStatesTitle);
    const usStatesReq = https.get(usStatesUrl, function (res) {
        res.pipe(usStatesCsv);
        usStatesCsv.on("finish", function () {
            usStatesCsv.close();
        });
    });

    const usCountiesTitle = "us-counties.csv";
    const usCountiesUrl = baseUrl + usCountiesTitle;
    const usCountiesCsv = fs.createWriteStream(path + "/" + usCountiesTitle);
    const usCountiesReq = https.get(usCountiesUrl, function (res) {
        res.pipe(usCountiesCsv);
        usCountiesCsv.on("finish", function () {
            usCountiesCsv.close();
        });
    });

    s3.upload(
        { Bucket: s3Bucket, Key: newS3Key + usStatesTitle, Body: usStatesCsv },
        function (err, data) {
            if (err) {
                throw err;
            } else {
                console.log("Upload Success", data.Location);
            }
        }
    );

    s3.upload(
        {
            Bucket: s3Bucket,
            Key: newS3Key + usCountiesTitle,
            Body: usCountiesCsv,
        },
        function (err, data) {
            if (err) {
                throw err;
            } else {
                console.log("Upload Success", data.Location);
            }
        }
    );
};