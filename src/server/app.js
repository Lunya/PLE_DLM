"use strict";
const express = require('express');
const http = require('http');
const path = require('path');
const bodyParser = require('body-parser');
let createEngine = require('node-twig').createEngine;
const hbase = require('hbase-rpc-client');

process.env.SERVER_PORT = 4200;

const app = express();

app.engine('.twig', createEngine({
	root: path.join(__dirname, './views'),
}));
app.set('views', './views');
app.set('view engine', 'twig');
app.use(bodyParser.json());


let client = hbase({
	zookeeperHosts: [ 'beetlejuice:2181' ],
	zookeeperRoot: '/hbase',
	//zookeeperReconnectTimeout: 20000,
	//rootRegionZKPath: '/hbase',
	rpcTimeout: 30000,
	//callTimeout: 5000,
	//tcpNoDelay: false,
	//tcpKeepAlive: true
});
client.on('error', (err) => console.log("ERROR: ",  err));
/*let get = new hbase.Get('r1');
client.get('DLM', get, (err, res) => {
	console.log("Error: ", err, "  Result: ", res);
});*/
let scan = client.getScanner('dlm');
let filter = {
	singleColumnValueFilter: {
		columnFamily: 'h',
		columnQualifier: 'la',
		compareOp: 'EQUAL',
		comparator: {
			regexStringComparator: {
				pattern: '.*',
				patternFlags: '',
				charset: 'UTF-8'
			}
		},
		filterIfMissing: true,
		latestVersionOnly: true
	}
};
let filterList = new hbase.FilterList();
filterList.addFilter(filter);
scan.setFilter(filterList);
scan.toArray((err, res) => {
	console.log('err: ', err, '  res: ', res);
});

app.get('/viewer', (req, res) => {
	res.status(200).render('viewer', {
		context: {}
	});
});
function bytearrayToInt16(bytearray) {
	return Uint16Array.from(bytearray)[0];
}
app.get('/hbase/list', (req, res) => {
	res.contentType('application/json');
	console.log('We want a list');
	let scan = client.getScanner('dlm');
	let filter = {
		singleColumnValueFilter: {
			columnFamily: 'h',
			columnQualifier: 'la',
			compareOp: 'EQUAL',
			comparator: {
				regexStringComparator: {
					pattern: '.*',
					patternFlags: '',
					charset: 'UTF-8'
				}
			},
			filterIfMissing: true,
			latestVersionOnly: true
		}
	};
	let filterList = new hbase.FilterList();
	filterList.addFilter(filter);
	scan.setFilter(filterList);
	scan.toArray((error, result) => {
		console.log('We get the list');
		console.log('h:la ', result[0]['cols']['h:la']);
		console.log('column 1 ', result[0].columns[1]);
		const mappedRes = result.map(e => {
			return { 'row': e.row, 'h:la': bytearrayToInt16(e.cols['h:la'].value), 'h:lo': bytearrayToInt16(e.cols['h:lo'].value) };
		});
		console.log('The list: ', mappedRes);
		res.status(200).send(mappedRes);
	});
});
app.get('/hbase/:lat/:lng/:level', (req, res) => {
	let get = new hbase.Get('r' + req.params.level);
	get.addColumns('h', 'h');
	get.addColumns('h', 'la');
	get.addColumns('h', 'lo');
	get.addColumns('h', 'h');
	client.get('DLM', get, (error, result) => {

	});
	const imageSize = 256;
	const imageTypeSize = 2;
	let imgArray = new Uint16Array(imageSize * imageSize);

	for (let y = 0; y < imageSize; y++) {
		for (let x = 0; x < imageSize; x++) {
			imgArray[y * imageSize + x] = y * imageSize + x;
		}
	}
	res.status(200).end(new Buffer(imgArray.buffer), 'binary');
});

app.get('/test', (req, res) => {
	res.status(200).render('index', {
		context: {
			projectName: 'PLE_DLM'
		}
	});
});

app.use(express.static(path.join(__dirname, './static')));
app.use('/ajax.js', express.static(path.join(__dirname, './node_modules/client-ajax/index.js')));

app.get('/', (req, res) => {
	res.contentType('text/html');
	res.status(200).send("Hello world!");
});

app.post('/image', (req, res) => {
	console.log(req.body);
	console.log(`lat: ${req.body.lat} lng: ${req.body.lng} img: ${req.body.img}`);
	const imageSize = 256;
	const imageTypeSize = 2;
	//let imgBuffer = new ArrayBuffer(imageSize * imageSize * imageTypeSize);
	/*let imgBuffer = new Buffer(imageSize * imageSize * imageTypeSize);
	let imgArray = new Uint16Array(imgBuffer);*/
	let imgArray = new Uint16Array(imageSize * imageSize);

	for (let y = 0; y < imageSize; y++) {
		for (let x = 0; x < imageSize; x++) {
			imgArray[y * imageSize + x] = y * imageSize + x;
		}
	}

	//res.contentType('application/octet-stream');
	res.status(200).end(new Buffer(imgArray.buffer), 'binary');
});

const server = http.createServer(app);
server.listen(process.env.NODE_PORT || 4200,
	() => console.log(`Server running on localhost:${process.env.NODE_PORT || 4200}`));
