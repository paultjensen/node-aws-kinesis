/*global Promise*/
/*global Set*/
/*global Buffer*/
/*global require*/

/* Kinesis Data Access
 * kinesis.js
 *
 * Purpose: Provides functions for Kinesis using the AWS SDK.
 * Author: Paul Jensen (paul.t.jensen@gmail.com)
 */

(function() {
    'use strict';

    let Aws = require('aws-sdk');
    let Logger = require('../lib/logger');
    let _config = require('../lib/config').config;

    let _stream = null;
    let kinesis = {};

    var _getStreamClient = function() {
        Logger.log.debug('Getting new Kinesis stream client for region: ' +
            _config.kinesis_region);
        _stream = new Aws.Kinesis({region: _config.kinesis_region});
    };

    kinesis.init = function(config) {
        if (config) {
            _config = config;
        }
    };

    kinesis.writeToStream = function(streamName, streamData) {
        Logger.log.debug('Writing to stream: ' + streamName);
        Logger.log.debug('Data:  ' + JSON.stringify(streamData));

        return new Promise(function(resolve, reject) {

            if (! _config) {
                reject('Kinesis: Not initialized. Config is null.');
                return;
            }

            if (streamData.length === 0) {
                reject('No data to write to stream');
                return;
            }

            try {
                if (!_stream) {
                    _getStreamClient();
                }
                const MAX_PUT_RECORDS_SIZE = 500;
                let i, j, parsedDataChunk, chunk = MAX_PUT_RECORDS_SIZE;
                for (i = 0, j = streamData.length; i < j; i += chunk) {
                    parsedDataChunk = streamData.slice(i, i + chunk);

                    let records = [];
                    for (let k = 0; k < parsedDataChunk.length; k++) {
                        let partitionKey = 'xxxxxxxx-xxxx-xxxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                            let r = Math.random()*16|0, v = c === 'x' ? r : (r&0x3|0x8);
                            return v.toString(16);
                        });

                        records.push(
                            {
                                "Data": JSON.stringify(parsedDataChunk[k]),
                                "PartitionKey": partitionKey
                            }
                        );
                    }

                    let recordsParams = {
                        Records: records,
                        StreamName: streamName
                    };

                    Logger.log.debug('Putting records to Kinesis stream.');
                    _stream.putRecords(recordsParams, (err) =>{
                        if (err) {
                            Logger.log.error(err);
                            reject(err);
                            return;
                        }
                        Logger.log.debug('Successfully wrote to Kinesis stream.');
                        resolve('success');
                    });
                }
            } catch(error) {
                Logger.log.error(error);
            }
        });
    };

    kinesis.getStreamList = function(params) {
        Logger.log.debug('Getting stream list');

        return new Promise(function(resolve, reject) {
            if (! _config) {
                reject('Kinesis: Not initialized. Config is null.');
                return;
            }

            if (!_stream) {
                _getStreamClient();
            }
            try {
                _stream.listStreams(params, function(error, streams) {
                    if (error) {
                        Logger.log.error(error);
                        reject(error);
                        return;
                    }
                    resolve(streams) ;
                });
            } catch (error) {
                Logger.log.error(error);
                reject(error);
            }
        });
    };

    kinesis.getShardIterators = function(streamName) {
        Logger.log.debug('Reading from stream: ' + streamName);

        return new Promise(function(resolve, reject) {
            if (! _config) {
                reject('Kinesis: Not initialized. Config is null.');
                return;
            }

            if (!_stream) {
                _getStreamClient();
            }

            try {
                _stream.describeStream({StreamName: streamName}).promise().then((result) => {
                    if (!result.StreamDescription.Shards.length) {
                        throw new Error('No shards!')
                    }

                    Logger.log.debug('Found ' + result.StreamDescription.Shards.length+ ' shards');
                    return result.StreamDescription.Shards.map((x) => x.ShardId);
                }).then((shardIds) => {
                    let shardIterators = shardIds.map((shardId) => {
                        const params = {
                            ShardId: shardId,
                            ShardIteratorType: 'TRIM_HORIZON',
                            StreamName: streamName,
                        };
                        return _stream.getShardIterator(params).promise().then((result) => {
                            Logger.log.debug('Got iterator id:' + result.ShardIterator);
                            return result.ShardIterator
                        });
                    });
                    return Promise.all(shardIterators);
                }).then((shardIterators) => {
                    resolve(shardIterators);
                });
            } catch (error) {
                Logger.log.error(error);
                reject(error);
            }
        });
    };

    kinesis.readStream = function(streamName, shardIterator, recordsToFetch) {
        Logger.log.debug('Reading from stream: ' + streamName);

        return new Promise(function(resolve, reject) {
            if (! _config) {
                reject('Kinesis: Not initialized. Config is null.');
                return;
            }

            if (!_stream) {
                _getStreamClient();
            }

            try {
                _stream.describeStream({StreamName: streamName}).promise().then((result) => {
                    if (!result.StreamDescription.Shards.length) {
                        throw new Error('No shards!')
                    }

                    Logger.log.debug('Found ' + result.StreamDescription.Shards.length+ ' shards');
                    return result.StreamDescription.Shards.map((x) => x.ShardId);
                }).then((shardIds) => {
                    let shardIterators = shardIds.map((shardId) => {
                        const params = {
                            ShardId: shardId,
                            ShardIteratorType: 'TRIM_HORIZON',
                            StreamName: streamName,
                        };
                        return _stream.getShardIterator(params).promise().then((result) => {
                            Logger.log.debug('Got iterator id:' + result.ShardIterator);
                            return result.ShardIterator
                        });
                    });
                    return Promise.all(shardIterators);
                }).then((shardIterators) => {

                    shardIterators.forEach((shardIterator) => {
                        let params = {
                            ShardIterator: shardIterator,
                            Limit: recordsToFetch
                        };
                        _stream.getRecords(params, function(error, streamData) {
                            if (error) {
                                Logger.log.error(error);
                                reject(error);
                                return;
                            }
                            resolve(streamData) ;
                        });
                    });
                });
            } catch (error) {
                Logger.log.error(error);
                reject(error);
            }
        });
    };

    kinesis.decodePayload = function(payload) {
        // noinspection JSPotentiallyInvalidConstructorUsage
        return new Buffer.from(payload, 'base64').toString('utf-8');
    };

    module.exports = {kinesis: kinesis};

})();