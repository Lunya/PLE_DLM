const express = require('express');
const http = require('http');
const path = require('path');
const createEngine = require('node-twig').createEngine;
const hbase = require('hbase');

process.env.SERVER_PORT = 4200;

const app = express();


app.engine('.twig', createEngine({
	root: path.join(__dirname, './views'),
}));
app.set('views', './views');
app.set('view engine', 'twig');

app.get('/test', (req, res) => {
	res.status(200).render('index', {
		context: {
			name: 'world'
		}
	});
});

app.use(express.static(path.join(__dirname, './static')));

app.get('/', (req, res) => {
	res.contentType('text/html');
	res.status(200).send("Hello world!");
});

const server = http.createServer(app);
server.listen(process.env.NODE_PORT || 4200,
	() => console.log(`Server running on localhost:${process.env.NODE_PORT || 4200}`));
