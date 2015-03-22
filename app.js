var fs = require('fs');
var MongoClient = require('mongodb').MongoClient;
var CSV = require('comma-separated-values');

var NAME_MONGO_DB = 'eijiro';
var NAME_MONGO_COLLECTION = 'words';
var url = 'mongodb://localhost:27017/' + NAME_MONGO_DB;

var inputCsv = process.argv[2];
var text = fs.readFileSync(inputCsv, 'utf-8');


/**
 * case1 synchronously
 */
// // data: example [ {a : 1}, {a : 2}, {a : 3} ]
// var insertDocuments = function(db, data, callback) {
// 	// Get the documents collection
// 	var collection = db.collection(NAME_MONGO_COLLECTION);
// 	// Insert some documents
// 	collection.insert(data, function(err, result) {
// 		if (err) return console.error(err);
// 		callback(result);
// 	});
// };
// MongoClient.connect(url, function(err, db) {
// 	if (err) return console.error(err);
//
// 	var rows = new CSV(text, { header: true, cast: false }).parse();
//
// 	console.log('Connected correctly to server');
//
// 	insertDocuments(db, rows, function(result) {
// 		console.log('Insert Finish! result is ', result);
// 		db.close();
// 	});
// });


/**
 * case2 streaming parse and use bulk insert (much faster)
 */
var c = 0;
MongoClient.connect(url, function(err, db) {
	if (err) return console.error(err);

	var col = db.collection(NAME_MONGO_COLLECTION);
	var batch = col.initializeUnorderedBulkOp({ useLegacyOps: true });

	// header is [ word,trans,level,memory,modify,pron ]
	new CSV(text, { header: true, cast: ['String', 'String', 'Number', 'Number', 'Number', 'String'] }).forEach(function(obj) {
		++c; if (c % 10000 === 0) console.log('count is ', c); // output debug log
		batch.insert(obj);
	});

	console.log('[batch.execute] start!!');

	batch.execute(function(err, result) {
		db.close();
		if (err) console.err(err);
		console.log('[batch.execute] end!! result is ', JSON.stringify(result));
		process.exit();
	});
});
