/*global Promise*/
/*global Set*/
/* Kinesis Data Producer Example
 * producer.js
 *
 * Purpose: Examples to exercise functions for Kinesis using the AWS SDK.
 * Author: Paul Jensen (paul.t.jensen@gmail.com)
 */

(function() {
    'use strict';

    let Logger = require('./lib/logger');
    let Kinesis = require('./lib/kinesis').kinesis;

    Kinesis.init();
    let streamName = 'test-stream';
    let streamData = [{ 'date': Date.now()}];
    Kinesis.getStreamList().then((streams)=> {
        Logger.log.info(JSON.stringify(streams));
    }).then(() => {
        return Kinesis.writeToStream(streamName, streamData).then((result) => {
            Logger.log.info(result);
        });
    }).catch((err) => {
        Logger.log.error(err);
    });
})();
