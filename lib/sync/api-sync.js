var _ = require('underscore');
var async = require('async');
var syncUtil = require('./util');
var DatasetClient = require('./DatasetClient');
var util = require('util');

var interceptors, ackQueue, pendingQueue, syncStorage;

var datasetClientsUpdateQueue = async.queue(function(datasetClientJson, cb){
  syncStorage.upsertDatasetClient(datasetClientJson.id, datasetClientJson, cb);
}, 50);
/**
 * Add the given items to the given queue. For each item, also add the given extraParams.
 * @param {Array} items the items to add to the queue
 * @param {Object} extraParams extra data that should be added to each item
 * @param {MongodbQueue} targetQueue the queue to push the messages to
 * @param {Function} cb the callback function
 */
function addToQueue(items, extraParams, targetQueue, cb) {
  if (!items || items.length === 0) {
    return cb();
  }
  var itemsToPush = _.map(items, function(item){
    return _.extend({}, item, extraParams);
  });
  syncUtil.doLog(syncUtil.SYNC_LOGGER, "debug", "adding " + itemsToPush.length + " items to queue " + targetQueue.getName());
  targetQueue.addMany(itemsToPush, cb);
}

/**
 * Reformat the give processedUpdates array to an object format that is expected by the sync client
 * @param {Array} processedUpdates the array of updates to process
 * @returns {Object} an object that contains the updates with different types
 */
function formatUpdates(processedUpdates) {
  var updates = {
    hashes: {}
  };
  _.each(processedUpdates, function(update){
    var type = update.type;
    var hash = update.hash;
    updates.hashes[hash] = update;
    updates[type] = updates[type] || {};
    updates[type][hash] = update;
  });
  return updates;
}

/**
 * Remove all the records in `updatesInRequest` from `updatesInDb` if they exist
 * @param {Array} updatesInDb
 * @param {Array} updatesInRequest
 * @returns 
 */
function removeUpdatesInRequest(updatesInDb, updatesInRequest) {
  var updatesNotInRequest = _.filter(updatesInDb, function(dbUpdate){
    var foundInRequest = _.findWhere(updatesInRequest, {hash: dbUpdate.hash});
    return !foundInRequest;
  });
  return updatesNotInRequest;
}

function processSyncAPI(datasetId, params, readDatasetClient, cb) {
  var queryParams = params.query_params || {};
  var metaData = params.meta_data || {};
  var cuid = syncUtil.getCuid(params);
  var datasetClient = new DatasetClient(datasetId, {queryParams: queryParams, metaData: metaData});
  var datasetClientFields = {id: datasetClient.getId(), datasetId: datasetId,  queryParams: queryParams, metaData: metaData, lastAccessed: Date.now()};
  async.parallel({
    pushDatasetClient: function(callback) {
      datasetClientsUpdateQueue.push(datasetClientFields);
      return callback();
    },
    addAcks: function(callback) {
      syncUtil.doLog(datasetId, 'debug', 'adding acks to queue. size = ' + (params.acknowledgements && params.acknowledgements.length || 0));
      addToQueue(params.acknowledgements, {datasetId: datasetId, cuid: cuid}, ackQueue, callback);
    },
    addPendings: function(callback) {
      syncUtil.doLog(datasetId, 'debug', 'adding pendings to queue. size = ' + (params.pending && params.pending.length || 0));
      addToQueue(params.pending, {datasetId: datasetId, cuid: cuid, meta_data: metaData}, pendingQueue, callback);
    },
    processedUpdates: function(callback) {
      syncUtil.doLog(datasetId, 'debug', 'list updates for client cuid = ' + cuid);
      syncStorage.listUpdates(datasetId, {cuid: cuid}, callback);
    }
  }, function(err, results){
    if (err) {
      syncUtil.doLog(datasetId, 'error', 'sync request error = ' + util.inspect(err), params);
      return cb(err);
    } else {
      syncUtil.doLog(datasetId, 'debug', 'syn API results ' + util.inspect(results));
      var globalHash = readDatasetClient? readDatasetClient.globalHash: undefined;
      //the acknowledgements in the current request will be put on the queue and take some time to process, so don't return them if they are still in the db
      var remainingUpdates = removeUpdatesInRequest(results.processedUpdates, params.acknowledgements);
      var response = {hash: globalHash, updates: formatUpdates(remainingUpdates)};
      interceptors.responseInterceptor(datasetId, queryParams, function(err){
        if (err) {
          syncUtil.doLog(datasetId, 'debug', 'sync response interceptor returns error = ' + util.inspect(err), params);
          return cb(err);
        }
        syncUtil.doLog(datasetId, 'debug', 'sync API response ' + util.inspect(response));
        return cb(null, response);
      });
    }
  });
}

/**
 * Process the sync request. It will
 * - validate the request body via the requestInterceptor
 * - check if the dataset client is stopped for sync
 * - create or update the dataset client
 * - process acknowledgements, push each of them to the ackQueue
 * - process pending changes, push each of them to the pendingQueue
 * - list any processed updates for the given client
 * @param {String} datasetId the id of the dataset
 * @param {Object} params the request body, it normally contain those fields:
 * @param {Object} params.query_params the query parameter for the dataset from the client
 * @param {Object} params.meta_data the meta data for the dataset from the client
 * @param {Object} params._fh an object added by the client sdk. it could have the `cuid` of the client
 * @param {Array} params.pending the pending changes array
 * @param {Array} params.acknowledgements the acknowledgements from the client
 * @param {Function} cb the callback function
 */
function sync(datasetId, params, cb) {
  syncUtil.doLog(datasetId, 'debug', 'process sync request for dataset ' + datasetId);
  var queryParams = params.query_params || {};
  var metaData = params.meta_data || {};
  var datasetClient = new DatasetClient(datasetId, {queryParams: queryParams, metaData: metaData});
  syncUtil.doLog(datasetId, 'debug', 'processing sync API request :: query_params = ' + util.inspect(queryParams) + ' :: meta_data = ' + util.inspect(metaData));
  async.series({
    requestInterceptor: async.apply(interceptors.requestInterceptor, datasetId, params),
    readDatasetClient: function checkDatasetclientStopped(callback) {
      syncStorage.readDatasetClient(datasetClient.getId(), function(err, datasetClientJson){
        if (err) {
          return callback(err);
        }
        if (datasetClientJson && datasetClientJson.stopped === true) {
          return callback(new Error('sync stopped for dataset ' + datasetId));
        } else {
          return callback(null, datasetClientJson);
        }
      });
    }
  }, function(err, results){
    if (err) {
      syncUtil.doLog(datasetId, 'debug', 'sync request returns error = ' + util.inspect(err), params);
      return cb(err);
    }
    return processSyncAPI(datasetId, params, results.readDatasetClient, cb);
  });
}

module.exports = function(interceptorsImpl, ackQueueImpl, pendingQueueImpl, syncStorageImpl){
  interceptors = interceptorsImpl;
  ackQueue = ackQueueImpl;
  pendingQueue = pendingQueueImpl;
  syncStorage = syncStorageImpl;
  return sync;
}
