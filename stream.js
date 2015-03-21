/*
 * Work in progress
 */

var fs = require('fs');
var _ = require('lodash');
var csv = require('csv');
var parse = require('csv').parse;
var transform = require('stream-transform');

var inputCsv = process.argv[2];
var readableStream = fs.createReadStream(inputCsv);

var parser = parse({ delimiter: ',' });
var transformer = transform(function(record, callback) {
	callback(null, JSON.stringify(record) + '\n');
});

readableStream.pipe(parser).pipe(transformer).pipe(process.stdout);


