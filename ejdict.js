var fs = require('fs');
var _ = require('lodash');
var unirest = require('unirest');
var async = require('async');
var CSV = require('comma-separated-values');
var sqlite3 = require('sqlite3').verbose();
var start = Date.now();

/**
 * new DB
 */
var DB_NAME = 'db/titan-jp.db';
var TABLE_NAME = 'word';
var COLUMES_NAME = 'spell, meaning, level';
var QUERY_DROP_TABLE = 'drop table ' + TABLE_NAME;
var QUERY_CREATE_TABLE = 'create table ' + TABLE_NAME + ' (id INTEGER PRIMARY KEY AUTOINCREMENT, ' + COLUMES_NAME + ' )';
var QUERY_INSERT_WORD = 'insert into ' + TABLE_NAME + ' ( ' + COLUMES_NAME + ' ) values (?,?,?)';


// eijiro db info
var DB_EIJIRO_FILENAME = 'db/eijiro.db';
var COLUMN_EIJIRO_WORD = 'word';
var level = process.argv[2]; if (! level) return console.error('required param "(int) LEVEL"');
var QUERY_EIJIRO_SELECT = 'SELECT ' + COLUMN_EIJIRO_WORD + ' FROM words WHERE level = ' + level + ' order by ' + COLUMN_EIJIRO_WORD;


/**
 * ejdict db info (public domain)
 * word, mean, level
 */
var EJDICT_DB_NAME = 'db/ejdict.sqlite3';
var EJDICT_TABLE_NAME = 'items';

async.waterfall([
  // read word from sqlite eijiro db
  function(cb) {
    var db = new sqlite3.Database(DB_EIJIRO_FILENAME);
    db.all(QUERY_EIJIRO_SELECT, function(err, rows) {
      console.log('will be inserted ' + rows.length + 'rows');
      db.close(function() {
        cb(null, _.pluck(rows, COLUMN_EIJIRO_WORD));
      });
    })
  },
  // create word table on db if not exists
  function(eijiroWords, cb) {
    var db = new sqlite3.Database(DB_NAME);
    db.get("select count(*) as count from sqlite_master where type='table' and name='" + TABLE_NAME + "';", function(err, row) {
      if (row.count === 0) {
        db.run(QUERY_CREATE_TABLE, function() {
          cb(err, db, eijiroWords);
        });
      } else {
        cb(err, db, eijiroWords);
      }
    });
  },
  // save word
  function(newDb, eijiroWords, cb) {
    newDb.run("begin transaction");
    var stmt = newDb.prepare(QUERY_INSERT_WORD);

    var ejdictDb = new sqlite3.Database(EJDICT_DB_NAME);
    async.each(eijiroWords, function(word, cb) {
      var originalWord = word;
      if (word.indexOf("'") >= 0) word = word.replace(/'/g, "''");

      ejdictDb.get("select * from " + EJDICT_TABLE_NAME + " where word = '" + word + "';", function(err, row) {
        console.log(row);
        if (_.isEmpty(row)) {
          console.log('Empty word::: ', word, row);
          cb(null);
        } else {
          stmt.run([
            originalWord,
            row.mean,
            level // this is Eijiro's level
          ]);
          cb(null);
        }
      });
    }, function(err) {
      if (err) return cb(err);

      stmt.finalize();
      newDb.run("commit");
      cb(null);
    });
  }
], function(err, result) {
  if (err) console.error(err);
  console.log((Date.now() - start) + "ms");
});
