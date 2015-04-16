
var influxRequest = require('./lib/InfluxRequest.js');
var url           = require('url');
var _             = require('underscore');

var defaultOptions = {
  hosts               : [],
  disabled_hosts      : [],
  username            : 'root',
  password            : 'root',
  port                : 8086,
  depreciatedLogging  : (process.env.NODE_ENV === undefined || 'development') ? console.log : false,
  failoverTimeout     : 60000,
  requestTimeout      : null,
  maxRetries          : 2,
  timePrecision       : 'ms',
  retentionPolicy     : 'default'
};

var InfluxDB = function(options) {

  this.options = _.extend(_.clone(defaultOptions), options);

  this.request = new influxRequest({
    failoverTimeout   : this.options.failoverTimeout,
    maxRetries        : this.options.maxRetries,
    requestTimeout    : this.options.requestTimeout
  });

  if ( (!_.isArray(this.options.hosts) || 0 === this.options.hosts.length ) && 'string' === typeof this.options.host)
  {
    this.request.addHost(this.options.host,this.options.port);
  }
  if (_.isArray(this.options.hosts) && 0 < this.options.hosts.length)
  {
    var self = this;
    _.each(this.options.hosts,function(host){
      self.request.addHost(host.host, host.port || self.options.port);
    });
  }

  return this;
};

InfluxDB.prototype._parseCallback = function(callback) {
  return function(err, res, body) {
    if ('undefined' === typeof callback) return;
    if(err) {
      return callback(err);
    }
    if(res.statusCode < 200 || res.statusCode >= 300) {
      return callback(new Error(body));
    }
    return callback(null, body);
  };
};

InfluxDB.prototype.setRequestTimeout = function (value)
{
  return this.request.setRequestTimeout(value);
};

InfluxDB.prototype.setFailoverTimeout = function (value)
{
  return this.request.setFailoverTimeout(value);
};


InfluxDB.prototype.url = function(database, query) {

  return url.format({
    pathname: database,
    query: _.extend({
      u: this.options.username,
      p: this.options.password,
      time_precision: this.options.timePrecision
    }, query || {})
  });
};

InfluxDB.prototype.createDatabase = function(databaseName, callback) {
  var query = 'create database ' + databaseName;
  this.query(query, callback);
};

InfluxDB.prototype.deleteDatabase = function(databaseName, callback) {
  var query = 'delete database ' + databaseName;
  this.query(query, callback);
};

InfluxDB.prototype.getDatabases = function(callback) {
  var query = 'show databases';
  this.query(query, callback);
};

InfluxDB.prototype.getMeasurements = function(options, callback) {
  callback = callback || options;
  if (typeof options === 'function'){
    options = {};
  }
  var query = 'show measurements ' + options.query;

  this.query(query, options, callback);
};

InfluxDB.prototype.getSeries = function(options, callback) {
  callback = callback || options;
  if (typeof options === 'function'){
    options = {};
  }
  var query = 'show series ' + options.query;
  
  this.query(query, options, callback);
};

InfluxDB.prototype.getUsers = function(options, callback) {
  callback = callback || options;
  if (typeof options === 'function'){
    options = {};
  }

  var query = 'show users';
  this.query(query, options, callback);
};

InfluxDB.prototype.createUser = function(username, password, options, callback) {
  callback = callback || options;
  if (typeof options === 'function'){
    options = {};
  }
  var query = 'create user ' + username + ' with password ' + password;
  
  this.query(query, options, callback);
};

/*InfluxDB.prototype.updateUser = function (databaseName, userName, options, callback)
{
  this.request.post({
    url: this.url('db/' + databaseName + '/users/' + userName),
    headers: {
      'content-type': 'application/json'
    },
    body: JSON.stringify(options, null)
  }, this._parseCallback(callback));
};*/

InfluxDB.prototype.writeSeries = function(series, tags, options, callback) {
  if(typeof options === 'function') {
    callback = options;
    options  = {};
  }
  options = options || {};

  var query = options.query || {};
  var data = {};
  data = series;
  data.tags = tags;
  data.database = options.database || this.options.database;
  data.retentionPolicy = options.retentionPolicy || this.options.retentionPolicy;

  console.log(data);

  this.request.post({
    url: this.seriesUrl(options.database,query),
    headers: {
      'content-type': 'application/json'
    },
    pool : 'undefined' !== typeof options.pool ? options.pool : {},
    body: JSON.stringify(data)
  }, this._parseCallback(callback));
};

InfluxDB.prototype.writePoint = function(seriesName, values, tags, options, callback) {
  var data = {};
  var dataValue = {};
  dataValue.fields = values;
  dataValue.name = values.name || seriesName;
  data.points = [dataValue];
  this.writeSeries(data, tags, options, callback);
};

InfluxDB.prototype.writePoints = function(seriesName, points, tags, options, callback) {
  var data = {points: points};
  _.each(data.points, function(p){
    var dataValue = {};
    dataValue.fields = p;
    dataValue.name = p.name || seriesName;
  });
  this.writeSeries(data, tags, options, callback);
};

InfluxDB.prototype.query = function(query, options, callback) {
  callback = callback || options;
  if (options === typeof 'function'){
    options = {};
  }
  var databaseName = options.database || this.options.database;

  this.request.get({
    url: this.url('query/', { q: query, db: databaseName}),
    json: true
  }, this._parseCallback(callback));
};

InfluxDB.prototype.dropSeries  = function(seriesName, options, callback) {
  callback = callback || options;
  if (options === typeof 'function'){
    options = {};
  }
  var query = 'drop ' + seriesName + ' ' + options.query;

  this.query(query, options, callback);
};

InfluxDB.prototype.getContinuousQueries = function(options, callback) {

  var query = 'show continuous queries';
  this.query(query, options, callback);
};


InfluxDB.prototype.dropContinuousQuery  = function(queryID, options, callback) {

  var query = 'drop continuous query ' + queryID;
  this.query(query, options, callback);
};


/*InfluxDB.prototype.getShardSpaces = function(databaseName, callback) {
  if ('function' === typeof databaseName) {
    callback = databaseName;
    databaseName = this.options.database;
  }
  this.request.get({
    url: this.url('cluster/shard_spaces'),
    json: true
  }, this._parseCallback(function(err, shards) {
    if (err) return callback(err, shards);
    callback(null, _.where(shards, {database: databaseName}));
  }));
};


InfluxDB.prototype.createShardSpace = function(databaseName, shardSpace, callback) {
  if ('function' === typeof shardSpace) {
    callback = shardSpace;
    shardSpace = databaseName;
    databaseName = this.options.database;
  }
  this.request.post({
    url: this.url('cluster/shard_spaces/' + databaseName),
    headers: {
      'content-type': 'application/json'
    },
    body: JSON.stringify(shardSpace, null)
  }, this._parseCallback(callback));
};


InfluxDB.prototype.updateShardSpace = function(databaseName, shardSpaceName, options, callback) {
  if ('function' === typeof options) {
    callback = options;
    options = shardSpaceName;
    shardSpaceName = databaseName;
    databaseName = this.options.database;
  }
  this.request.post({
    url: this.url('cluster/shard_spaces/' + databaseName + '/' + shardSpaceName),
    headers: {
      'content-type': 'application/json'
    },
    body: JSON.stringify(options, null)
  }, this._parseCallback(callback));
};


InfluxDB.prototype.deleteShardSpace = function(databaseName, shardSpaceName, callback) {
  if ('function' === typeof shardSpaceName) {
    callback = shardSpaceName;
    shardSpaceName = databaseName;
    databaseName = this.options.database;
  }
  this.request.get({
    method: 'DELETE',
    url: this.url('cluster/shard_spaces/' + databaseName + '/' + shardSpaceName)
  }, this._parseCallback(callback));
};*/


InfluxDB.prototype.seriesUrl  = function(databaseName,query) {
  return this.url('write',query);
};

InfluxDB.prototype.getHostsAvailable = function()
{
  return this.request.getHostsAvailable();
};

InfluxDB.prototype.getHostsDisabled = function()
{
  return this.request.getHostsDisabled();
};

var createClient = function() {
  var args = arguments;
  var Client = function () { return InfluxDB.apply(this, args); };
  Client.prototype = InfluxDB.prototype;
  return new Client();
};


var parseResult = function(res) {
  return _.map(res.points, function(point) {
    var objectPoint = {};
    _.each(res.columns, function(name, n) {
      objectPoint[name] = point[n];
    });
    return objectPoint;
  });
};

module.exports = createClient;
module.exports.parseResult = parseResult;
module.exports.InfluxDB = InfluxDB;
module.exports.defaultOptions = defaultOptions;
