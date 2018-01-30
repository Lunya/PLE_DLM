"use strict";
const express = require('express');
const http = require('http');
const path = require('path');
const bodyParser = require('body-parser');
let createEngine = require('node-twig').createEngine;
const hbase = require('hbase-rpc-client');
const Geohash = require('latlon-geohash');
const geohash = require('ngeohash');


process.env.NODE_PORT = 7531;

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
function bytearrayToString(bytearray) {
	const intArray = Uint8Array.from(bytearray);
	let str = '';
	for (let i = 0; i < intArray.length; ++i)
		str += String.fromCharCode(intArray[i]);
	return str;
}
function bytearrayToInt16(bytearray) {
	return Uint16Array.from(bytearray)[0];
}
function bytearrayToFloat32(bytearray) {
	return Float32Array.from(bytearray)[0];
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
			return { 'row': bytearrayToString(e.row), 'h:la': bytearrayToFloat32(e.cols['h:la'].value), 'h:lo': bytearrayToFloat32(e.cols['h:lo'].value) };
		});
		console.log('The list: ', mappedRes);
		res.status(200).send(mappedRes);
	});
});
app.get('/hbase/:row', (req, res) => {
	let get = new hbase.Get(req.params.row);
	get.addColumn('h', 'h');
	client.get('dlm', get, (error, result) => {
		const imageSize = 256;
		const imageTypeSize = 2;
		let imgArray = new Uint16Array(imageSize * imageSize);
		console.log(result.cols['h:h'].value);
		//res.status(200).end(new Buffer(imgArray.buffer), 'binary');
		res.status(200).end(result.cols['h:h'].value, 'binary');
	});
});
app.get('/hbase/:lat/:lng/:level', (req, res) => {
	let get = new hbase.Get('r' + req.params.level);
	get.addColumn('h', 'h');
	get.addColumn('h', 'la');
	get.addColumn('h', 'lo');
	get.addColumn('h', 'h');
	client.get('dlm', get, (error, result) => {

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

app.get('/gradientGenerator', (req, res) => {
	res.status(200).render('gradientGenerator', {});
});

app.get('/', (req, res) => {
	res.status(200).render('index', {
		context: {
			projectName: 'PLE_DLM'
		}
	});
});

app.use(express.static(path.join(__dirname, './static')));
app.use('/ajax.js', express.static(path.join(__dirname, './node_modules/client-ajax/index.js')));

app.post('/hbase/stats', (req, res) => {
	const geohashPrecision = 8;

	const topLeft = Geohash.encode(req.body.latMin, req.body.lngMin, geohashPrecision);
	const bottomRight = Geohash.encode(req.body.latMax, req.body.lngMax, geohashPrecision);

	const columnQualifier = new ArrayBuffer(1);
	columnQualifier[0] = req.body.zoom;
	let scan = client.getScanner('dlm');
	let filterTopLeft = {
		singleColumnValueFilter: {
			columnFamily: 'h',
			columnQualifier: columnQualifier,
			compareOp: 'GREATER_OR_EQUAL',
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
			return { 'row': bytearrayToString(e.row), 'h:la': bytearrayToFloat32(e.cols['h:la'].value), 'h:lo': bytearrayToFloat32(e.cols['h:lo'].value) };
		});
		console.log('The list: ', mappedRes);
		res.status(200).send(mappedRes);
	});

	res.contentType('application/json');
	res.status(200).end([{ position: '', zoom: 0, lat: 0.0, lng: 0.0 }]);
});
app.post('/hbase/heights', (req, res) => {
	const hash = geohash.encode(req.body.lat, req.body.lng, 8);
	let get = new hbase.Get(hash);
	get.addColumn('h', req.body.zoom.toString());
	/*const imageSize = 256;
	let imgArray = new Uint16Array(imageSize * imageSize);
	for (let y = 0; y < imageSize; y++)
		for (let x = 0; x < imageSize; x++)
			imgArray[y * imageSize + x] = y * imageSize + x;
	res.status(200).end(new Buffer(imgArray.buffer), 'binary');*/
	client.get('dlm', get, (error, result) => {
		//res.status(200).end(result.cols['h:' + req.body.zoom.toString()].value, 'binary');
	});
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
