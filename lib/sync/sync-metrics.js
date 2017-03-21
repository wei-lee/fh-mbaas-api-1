var fhComponentMetrics = require('fh-component-metrics');
var syncUtils = require('./util');
var metricsClient = fhComponentMetrics({enabled: false});
var _ = require('underscore');

var METRIC_KEYS = {
  WORKER_JOB_ERROR_COUNT: "worker-get-job-error-count",
  WORKER_JOB_TOTAL_COUNT: "worker-job-count",
  WORKER_JOB_FAILURE_COUNT: "worker-job-failure-count",
  WORKER_JOB_SUCCESS_COUNT: "worker-job-success-count",
  WORKER_JOB_PROCESS_TIME: "worker-job-process-time",
  QUEUE_OPERATION_TIME: "queue-operation-time",
  HANDLER_OPERATION_TIME: "sync-handler-operation-time",
  SYNC_SCHEDULER_CHECK_TIME: "sync-scheduler-check-time",
  SYNC_REQUEST_TOTAL_PROCESS_TIME: "sync-request-total-process-time",
  PENDING_CHANGE_PROCESS_TIME: "pending-change-process-time",
  SYNC_API_PROCESS_TIME: "sync-api-process-time",
  MONGODB_OPERATION_TIME: "mongodb-operation-time",
  WORKER_QUEUE_SIZE: "worker-queue-size"
};

var Timer = function(){
  this.start = Date.now();
};

Timer.prototype.stop = function(){
  var end = Date.now();
  return end - this.start;
};

var timeAsyncFunc = function(metricKey, targetFn) {
  return function() {
    var args = [].slice.call(arguments);
    if (typeof args[args.length - 1] !== 'function') {
      syncUtils.doLog(syncUtils.SYNC_LOGGER, 'debug', 'can not time the target function ' + targetFn.name + ' as last argument is not a function');
    } else {
      var callback = args.pop();
      var timer = new Timer();
      args.push(function(){
        var timing = timer.stop();
        metricsClient.gauge(metricKey, {success: !arguments[0], fn: targetFn.name}, timing);
        return callback.apply(null, arguments);
      });
    }
    targetFn.apply(null, args);
  }
};

/**
 * Compute the max, min, current, average values from the records
 * @param {Array} records 
 */
var aggregateData = function(metricName, records, filter, valueField, unit) {
  var returnValue = {'message': 'no stats available'};
  if (records && records.length > 0) {
    var result = _.chain(records).filter(filter).reduce(records, function(memo, record){
      var value = record.fields[valueField];
      memo.current = value;
      memo.numberOfRecords++;
      memo.total+=value;
      memo.max = Math.max(value, memo.max);
      memo.min = Math.min(value, memo.min);
      memo.from = Math.min(record.ts, memo.from);
      memo.end = Math.max(record.ts, memo.end);
    }, {max: 0, min:Number.MAX_SAFE_INTEGER, current: 0, numberOfRecords: 0, total: 0, from: 0, end: 0}).value();
    returnValue = {
      name: metricName,
      from: result.from.toISOString(),
      end: result.end.toISOString(),
      unit: unit,
      numberOfRecords: result.numberOfRecords,
      max: result.max,
      min: result.min,
      mean: Math.floor(result.total/result.numberOfRecords),
      current: result.current
    }
  }
  return returnValue;
};

var getStats = function(redisClient, numberOfRecords, cb) {
  var metricsToFetch = ['', METRIC_KEYS.WORKER_JOB_PROCESS_TIME, METRIC_KEYS.WORKER_QUEUE_SIZE, METRIC_KEYS.SYNC_API_PROCESS_TIME];
};

module.exports = {
  init: function(metricsConf, redisUrl, recordsToKeep) {
    var metricsConfig = metricsConf || {enabled: false};
    if (redisUrl) {
      metricsConfig.enabled = true;
      metricsClient.backends = metricsClient.backends || [];
      metricsClient.backends.push({
        type: 'redis',
        connection: {url: redisUrl},
        namespace: 'syncstats',
        recordsToKeep: recordsToKeep
      });
    }
    metricsClient = fhComponentMetrics(metricsConfig);
    return metricsClient;
  },
  KEYS: METRIC_KEYS,
  startTimer: function() {
    return new Timer();
  },
  timeAsyncFunc: timeAsyncFunc
};
