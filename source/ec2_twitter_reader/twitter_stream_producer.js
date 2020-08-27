/***
Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
***/

'use strict';

var config = require('./config');
var Twit = require('twit');
var util = require('util');
var logger = require('./util/logger');

var AWS = require('aws-sdk');
if (!AWS.config.region) {
  AWS.config.update({
    region: 'us-west-2'
  });
}

var ssm            = new AWS.SSM({region: AWS.config.region});
var secretsmanager = new AWS.SecretsManager();


function twitterStreamProducer(firehose) {
  var log = logger().getLogger('producer');

  var waitBetweenPutRecordsCallsInMilliseconds = 0 ;
  var topics           = '';
  var languages        = '';  

  console.log(config);
  var kinesis_delivery = config.kinesis_delivery;  
  
  var callbackMain = function(data, config){
	var T = new Twit(config);
    log.info(
      util.format(
        'Configured wait between consecutive PutRecords call in milliseconds: %d',
        waitBetweenPutRecordsCallsInMilliseconds
      )
    );
    console.log({ track: topics , language: languages });
	var stream = T.stream('statuses/filter', { track: topics , language: languages });
	var records = [];
	var record = {};
	var recordParams = {};
	stream.on('tweet', function (tweet) {
	  var tweetString = JSON.stringify(tweet);
	  recordParams = {
		DeliveryStreamName: kinesis_delivery,
		Record: {
		  Data: tweetString +'\n'
		}
	  };
	  firehose.putRecord(recordParams, function(err, data) {
		if (err) {
		  console.log(err);
		}
	  });
	  Promise.all([
		ssm.getParameter({ Name: "TwitterTermList", WithDecryption: true}).promise(), 	
		ssm.getParameter({ Name: "TwitterLanguages", WithDecryption: true}).promise()
		
	  ]).then(function(values) {
		topics    = values[0].Parameter.Value;
		languages = values[1].Parameter.Value;
        
        console.log({ track: topics , language: languages });
  	    stream = T.stream('statuses/filter', { track: topics , language: languages });
	  });  
	});	
	stream.on('error', function (err) {
	  console.log('Oh no')
	})


  }
 
  Promise.all([
    secretsmanager.getSecretValue({ SecretId: "AuthConsumerManagerSecret" }).promise(), 
    secretsmanager.getSecretValue({ SecretId: "AuthConsumerSecretManagerSecret" }).promise(), 
    secretsmanager.getSecretValue({ SecretId: "AuthAccessTokenManagerSecret" }).promise(), 
    secretsmanager.getSecretValue({ SecretId: "AuthAccessTokenSecretManagerSecret" }).promise(), 
	
    ssm.getParameter({ Name: "WaitBetweenPutRecordsCallsInMilliseconds", WithDecryption: true}).promise(), 	
    ssm.getParameter({ Name: "TwitterTermList", WithDecryption: true}).promise(), 	
    ssm.getParameter({ Name: "TwitterLanguages", WithDecryption: true}).promise()
    
  ]).then(function(values) {
    // return the result to the caller of the Lambda function
    var config = {
        consumer_key       : values[0].SecretString,
        consumer_secret    : values[1].SecretString,
        access_token       : values[2].SecretString,
        access_token_secret: values[3].SecretString
    }

    waitBetweenPutRecordsCallsInMilliseconds = values[4].Parameter.Value;
    topics    = values[5].Parameter.Value;
    languages = values[6].Parameter.Value;
	
    callbackMain(null, config);
  });
}
const twitterStreamProducerPromise = function(firehose) {
  return new Promise((resolve, reject) => {
	twitterStreamProducer(firehose);
    if ( true  ) {
      resolve("Stuff worked!");
    } else {
      reject(Error("It broke"));
    }
  });
}
module.exports = twitterStreamProducerPromise;
