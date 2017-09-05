/*
 ***** BEGIN LICENSE BLOCK *****
 
 This file is part of the Zotero Data Server.
 
 Copyright © 2017 Center for History and New Media
 George Mason University, Fairfax, Virginia, USA
 http://zotero.org
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
 ***** END LICENSE BLOCK *****
 */

const mysql2 = require('mysql2');
const mysql2Promise = require('mysql2/promise');
const sqlite = require('sqlite');
const through2 = require('through2');
const request = require('request');
const config = require('./config');

let done = false;
let inserted = 0;

function insert(db, hash, field, callback) {
	db.run('INSERT OR REPLACE INTO fields (hash, field) VALUES (?,?)',
		[hash, JSON.stringify(field)])
		.then(callback)
		.catch(callback);
	inserted++;
}

function streamShard(connectionInfo, db) {
	return new Promise(function (resolve, reject) {
		let connection = mysql2.createConnection(connectionInfo);
		
		connection.connect(function (err) {
			if (err) return reject(err);
			
			let sql = `
			SELECT DISTINCT itmat.storageHash, itmd.fieldID, itmd.value
			FROM itemAttachments AS itmat
			LEFT JOIN itemData AS itmd USING(itemID)
			WHERE itmat.storageHash IS NOT NULL
			`;
			
			connection.query(sql)
				.stream({highWaterMark: 1000})
				.pipe(through2({objectMode: true}, function (row, enc, next) {
					insert(db, row.storageHash, {fieldID: row.fieldID, value: row.value}, function () {
						next();
					});
				}))
				.on('data', function () {
				})
				.on('end', function () {
					resolve();
				})
		});
	});
}

// node index.js <shardID>
async function main() {
	console.time("total time");
	
	let db = await sqlite.open('./db.sqlite', {Promise});
	await db.run("CREATE TABLE IF NOT EXISTS fields (hash TEXT PRIMARY KEY, field TEXT)");
	await db.run("CREATE UNIQUE INDEX IF NOT EXISTS hash_field_index ON fields (hash, field)");
	
	let master = await mysql2Promise.createConnection({
		host: config.masterHost,
		user: config.masterUser,
		password: config.masterPassword,
		database: config.masterDatabase
	});
	
	let [shardRows] = await master.execute(
		"SELECT * FROM shards AS s LEFT JOIN shardHosts AS sh USING(shardHostID) WHERE shardID = ?",
		[process.argv[2]]
	);
	master.close();
	
	let shardRow = shardRows[0];
	
	let connectionInfo = {
		host: shardRow.address,
		user: config.masterUser,
		password: config.masterPassword,
		port: shardRow.port,
		database: shardRow.db
	};
	
	await db.run("BEGIN TRANSACTION");
	
	await streamShard(connectionInfo, db);
	
	await db.run("END TRANSACTION");
	await db.close();
	
	console.timeEnd("total time");
	done = true;
}

setInterval(function () {
	console.log('inserted: ' + inserted);
	if (done) process.exit();
}, 1000);

main();
