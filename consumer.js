
/*global Promise*/
/*global Set*/
/* Kinesis Data Access Example
 * producer.js
 *
 * Purpose: Examples to exercise functions for Kinesis using the AWS SDK.
 * Author: Paul Jensen (paul.t.jensen@gmail.com)
 */

(function() {
    'use strict';

    let Logger = require('./lib/logger');
    let Kinesis = require('./lib/kinesis').kinesis;
    const RECORDSPERREAD = 2;

    Kinesis.init();
    let streamName = 'test-stream';
    Kinesis.getShardIterators(streamName).then((shardIterators) => {
        shardIterators.forEach((shardIterator) => {
            _readStream(streamName, shardIterator, RECORDSPERREAD);
        });
    }).catch((err) => {
        Logger.log.error(err);
    });

    let _readStream = function(streamName, shardIterator, recordsToFetchPerRead) {
        return Kinesis.readStream(streamName, shardIterator, recordsToFetchPerRead).then((streamReadResult) => {
            if (streamReadResult && streamReadResult.Records) {
                streamReadResult.Records.forEach((record) => {
                    Logger.log.info('Sequence Number: ' + record.SequenceNumber);
                    Logger.log.info('Record Data:' + Kinesis.decodePayload(record.Data));
                });
            }
            if (streamReadResult.MillisBehindLatest > 0) {
                _readStream(streamName, streamReadResult.NextShardIterator, RECORDSPERREAD);
            }
        });
    };
})();