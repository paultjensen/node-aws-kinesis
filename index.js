/*global Promise*/
/*global Set*/
/* Kinesis Data Access Example
 * index.js
 *
 * Purpose: Examples to exercise functions for Kinesis using the AWS SDK.
 * Author: Paul Jensen (paul.t.jensen@gmail.com)
 */

(function() {
    'use strict';

    let Logger = require('./lib/logger');
    let Kinesis = require('./lib/kinesis').kinesis;
    const recordsToFetchPerRead = 2;

    Kinesis.init();
    let streamName = 'test-stream';
    let streamData = [{ 'foo': 'bar'}];
    Kinesis.getStreamList().then((streams)=> {
        Logger.log.info(JSON.stringify(streams));
    }).then(() => {
        return Kinesis.writeToStream(streamName, streamData).then((result) => {
            Logger.log.info(result);
        });
    }).then(() => {
        return Kinesis.getShardIterators(streamName);
    }).then((shardIterators) => {
        shardIterators.forEach((shardIterator) => {
            return Kinesis.readStream(streamName, shardIterator, recordsToFetchPerRead).then((streamReadResult) => {
                if (streamReadResult && streamReadResult.Records) {
                    streamReadResult.Records.forEach((record) => {
                        Logger.log.info('Sequence Number: ' + record.SequenceNumber);
                        Logger.log.info('Record Data:' + Kinesis.decodePayload(record.Data));
                    });
                }
            });
        });

    }).catch((err) => {
        Logger.log.error(err);
    });

})();