"use strict";
const express = require('express');
const http = require('http');
const path = require('path');
const bodyParser = require('body-parser');
let createEngine = require('node-twig').createEngine;
//const hbase = require('hbase');
const HBase = require('hbase-client');
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
let get = new hbase.Get('r1');
client.get('DLM', get, (err, res) => {
	console.log("Error: ", err, "  Result: ", res);
});
/*let client = HBase.create({
	zookeeperHosts: [
		'localhost:2181'
	],
	zookeeperRoot: '/hbase'
});
client.getRow('DLM', 'r1', (error, row) => {
	console.log("error:", error, "row:", row);
});
let table = hbase({ host: 'localhost', port: 2181 })
	.table('DLM');
let row = table.row('r1');
let rows = table.row('r*');
row.get('h', (error, value) => {
	console.log("error:", error, "value:", value);
});*/
app.get('/hbase', (req, res) => {
	/*let table = hbase({ host: 'localhost', port: 16010 })
		.table('DLM');
	let row = table.row('r1');
	let rows = table.row('r*');
	row.get('h', (error, value) => {
		console.log(error, value);
		res.contentType('text/html');
		res.send(error.body);
	});*/
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
