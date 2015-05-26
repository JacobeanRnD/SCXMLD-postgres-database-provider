'use strict';

var pg = require('pg'),
  async = require('async');

module.exports = function (opts) {
  var db = {};
  opts = opts || {};
  opts.connectionString = opts.connectionString || process.env.POSTGRES_URL || 'postgres://postgres:test@localhost:5432/scxmld';

  db.init = function (initialized) {
    pg.connect(opts.connectionString, function (connectError, client, done) {
      if(connectError){ 
        console.log('Postgres connection error', connectError);
        return initialized(connectError);
      }

      var schemas = [
        'CREATE TABLE IF NOT EXISTS' +
        ' instances(id varchar primary key,' +
        ' configuration JSON,' +
        ' created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW())',
        
        'CREATE TABLE IF NOT EXISTS' + 
        ' events(timestamp TIMESTAMP WITH TIME ZONE primary key DEFAULT NOW(),' +
        ' instanceId varchar REFERENCES instances(id) ON DELETE CASCADE,' +
        ' event JSON,' +
        ' snapshot JSON)',
        
        'CREATE TABLE IF NOT EXISTS' +
        ' metainfo(key varchar primary key,' +
        ' data JSON,' +
        ' created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW())'
      ];

      async.eachSeries(schemas, function (schema, next) {
        client.query(schema, next);
      }, function (err) {
        if(err) {
          console.log('Error initializing postgres.', err);
        }
        
        client.end();
        done();
        initialized(err);
      });
    });
  };

  db.query = function (config, queryDone) {
    pg.connect(opts.connectionString, function (connectError, client, done) {
      if(connectError) return queryDone(connectError);

      client.query(config, function (queryError, result) {
        //Give back the client to postgres client pool
        done();

        //Return the result
        if (queryDone) queryDone(queryError, result);
      });
    });
  };

  db.saveInstance = function (instanceId, conf, done) {
    db.updateInstance(instanceId, conf, function (error, result) {
      if(error) return done(error);

      if(result.rowCount > 0) return done();

      //Upsert
      db.query({
        text: 'INSERT INTO instances (id, configuration) VALUES($1, $2)',
        values: [instanceId, JSON.stringify(conf)]
      }, done);
    });
  };

  db.updateInstance = function (instanceId, conf, done) {
    db.query({
      text: 'UPDATE instances SET configuration = $1 WHERE id = $2',
      values: [JSON.stringify(conf), instanceId]
    }, done);
  };

  db.getInstance = function (instanceId, done) {
    db.query({
      text: 'SELECT * FROM instances WHERE id = $1',
      values: [instanceId]
    }, function (error, result) {
      if(error) return done(error);

      if(result.rowCount > 0)
        done(null, result.rows[0].configuration);
      else
        done({ statusCode: 404 });
    });
  };

  db.getInstances = function (done) {
    db.query({
      text: 'SELECT * FROM instances',
      values: []
    }, function (error, result) {
      if(error) return done(error);

      var instances = result.rows.map(function (instance) {
        return instance.id;          
      });

      done(null, instances);
    });
  };

  db.deleteInstance = function (instanceId, done) {
    db.query({
      text: 'DELETE FROM instances WHERE id = $1',
      values: [instanceId]
    }, function (error) {
      if(error) return done(error);

      done();
    });
  };

  db.saveEvent = function (instanceId, details, done) {
    db.query({
      text: 'INSERT INTO events (instanceId, event, snapshot, timestamp) VALUES($1, $2, $3, $4)',
      values: [instanceId, JSON.stringify(details.event), JSON.stringify(details.snapshot), details.timestamp]
    }, function (error) {
      if(error) return done(error);

      done();
    });
  };

  db.getEvents = function (instanceId, done) {
    db.query({
      text: 'SELECT * FROM events WHERE instanceId = $1',
      values: [instanceId]
    }, function (error, result) {
      if(error) return done(error);

      done(null, result.rows);
    });
  };

  db.set = function (key, value, done) {
    var values = [key, value];
    db.query({
      text : 'UPDATE metainfo SET data=$2 where key=$1;',
      values : values
    }, function(err, result){
      if(err) return done(err);
      if(result.rowCount > 0) return done();

      db.query({
        text : 'INSERT INTO metainfo (key, data) VALUES($1, $2);',
        values : values
      }, function(err, result){
        if(err) return done(err);

        done(null, result.rowCount > 0);
      }); 
    }); 
  };

  db.get = function (key, done) {
    db.query({
      text: 'SELECT * FROM metainfo WHERE key = $1',
      values: [key]
    }, function (error, result) {
      if(error) return done(error);

      if(result.rows.length){
        done(null, result.rows[0].data);
      }else{
        done(new Error('Unable to find container info'));
      }

    });
  };

  db.del = function (key, done) {
    db.query({
      text: 'DELETE FROM metainfo WHERE key = $1',
      values: [key]
    }, function (error) {
      if(error) return done(error);

      done(null, true);
    });
  };

  return db;
};