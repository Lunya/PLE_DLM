const express = require('express');
const http = require('http');
const path = require('path');
const bodyParser = require('body-parser');
let createEngine = require('node-twig').createEngine;
const hbase = require('hbase');

process.env.SERVER_PORT = 4200;

const app = express();


app.engine('.twig', createEngine({
	root: path.join(__dirname, './views'),
}));
app.set('views', './views');
app.set('view engine', 'twig');
app.use(bodyParser.json());

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
