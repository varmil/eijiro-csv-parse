var fs = require('fs');
var _ = require('lodash');
var CSV = require('comma-separated-values');

var DB_FILENAME = 'eijiro';
var TABLE_NAME = 'words';
// full : 'word, trans, level, memory, modify, pron'
var COLUMES_NAME = 'word, trans, level, pron';
var QUERY_DROP_TABLE = 'drop table ' + TABLE_NAME;
var QUERY_CREATE_TABLE = 'create table ' + TABLE_NAME + ' (id INTEGER PRIMARY KEY AUTOINCREMENT, ' + COLUMES_NAME + ' )';
var QUERY_INSERT_WORD = 'insert into ' + TABLE_NAME + ' ( ' + COLUMES_NAME + ' ) values (?,?,?,?)';
var CAST_ARRAY = ['String', 'String', 'Number', 'String'];


var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database(DB_FILENAME);

var inputCsv = process.argv[2];
var text = fs.readFileSync(inputCsv, 'utf-8');

var start = Date.now();
var c = 0;

// 1. EACH DB RUN : This is fast
// db.serialize(function() {
//   db.run("begin transaction");
//   db.run(QUERY_CREATE_TABLE);
//
//   new CSV(text, { header: true, cast: CAST_ARRAY }).forEach(function(obj) {
//     db.run(QUERY_INSERT_WORD, _.values(obj), function() {
//       ++c; if (c % 10000 === 0) console.log('count is ', c); // output debug log
//     });
//   });
//
//   db.run("commit");
// });


// 2. EACH FINALIZE : This is fast
// db.serialize(function() {
//   db.run("begin transaction");
//   db.run(QUERY_CREATE_TABLE);
//
//   new CSV(text, { header: true, cast: CAST_ARRAY }).forEach(function(obj) {
//     ++c; if (c % 10000 === 0) console.log('count is ', c); // output debug log
//
//     var stmt = db.prepare(QUERY_INSERT_WORD);
//     stmt.run(_.values(obj));
//     stmt.finalize();
//   });
//
//   db.run("commit");
// });


// 3. REUSE STMT : This is very very slow
db.serialize(function() {
  db.run("begin transaction");
  db.run(QUERY_CREATE_TABLE);

  var stmt = db.prepare(QUERY_INSERT_WORD);

  new CSV(text, { header: true, cast: CAST_ARRAY }).forEach(function(obj) {
    ++c; if (c % 10000 === 0) console.log('count is ', c); // output debug log
    stmt.run(_.values(obj));
  });

  stmt.finalize();
  db.run("commit");
});


db.close(function() {
  // sqlite3 has now fully committed the changes
  console.log((Date.now() - start) + "ms");
});
