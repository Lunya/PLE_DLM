const LATITUDE_MIN = -90.0;
const LATITUDE_MAX = 90.0;
const LONGITUDE_MIN = -180.0;
const LONGITUDE_MAX = 180.0;

let Point = function(x, y) {
	this.x = x || 0;
	this.y = y || 0;
};

let MapImage = function(data, size, position) {
	// private functions
	this.buildImage = function() {
		let canvas = document.createElement('canvas');
		let ctx = canvas.getContext('2d');
		canvas.width = this.size.x;
		canvas.height = this.size.y;
		let id = ctx.createImageData(this.size.x, this.size.y);
		let data = id.data;
		for (let i = 0, size = this.data.length; i < size; i++) {
			let I = i * 4;
			data[I] = this.data[i] / 256;// / 8000;
			data[I+1] = this.data[i] % 256;// / 8000;
			data[I+2] = this.data[i] / 256;// / 8000;
			data[I+3] = 255;
		}
		ctx.putImageData(id, 0, 0);36
		ctx.font = '22px monospace';
		ctx.fillStyle = '#ff0';
		ctx.strokeStyle = '#f0f';
		const text = `${this.position.x}E;${this.position.y}N`;
		ctx.fillText(text, 20, 42);
		ctx.strokeText(text, 20, 42);
		this._image.src = canvas.toDataURL('image/png');
	};

	// public functions
	this.getImage = function() {
		this._lastAccess = Date.now();
		return this._image;
	};

	// constructor
	this.data = data; // Bitmap
	this.size = size; // Point
	this.position = position; // Point
	this._image = new Image();
	this._lastAccess = Date.now();
}

let Map = function(parent) {
	// private functions
	this.resizeCanvas = function() {
		this.canvas.setAttribute('width', '0px');
		this.canvas.setAttribute('height', '0px');
		this.size.x = this.parent.clientWidth;
		this.size.y = this.parent.clientHeight;
		this.canvas.setAttribute('width', this.size.x + 'px');
		this.canvas.setAttribute('height', this.size.y + 'px');
	};

	this.update = function() {
		for (let i = 0; i < this.images.length; i++) {
			let image = this.images[i];
			this.ctx.drawImage(image.getImage(), image.position.x, image.position.y);
		}
		// draw meridians
	};

	// public functions

	this.write = function(position, text, size) {
		this.ctx.font = (size || 12) + 'px monospace';
		this.ctx.fillText(text, position.x, position.y);
	};

	// constructor
	this.parent = parent;

	this.canvas = document.createElement('canvas');
	this.ctx = this.canvas.getContext('2d');
	this.canvas.classList.add('map');
	this.parent.appendChild(this.canvas);

	this.images = [];

	this.zoom = 1.0; // zoom = pixel * degree
	this.size = new Point(this.parent.clientWidth, this.parent.clientHeight);
	this.resizeCanvas();

	// bindings
	window.addEventListener('resize', function(e) {
		this.resizeCanvas();
	}.bind(this));
	this.canvas.addEventListener('mousemove', function(e) {
		this.ctx.beginPath();
		this.ctx.arc(
			e.clientX - e.target.offsetLeft,
			e.clientY - e.target.offsetTop,
			5, 0, 2*Math.PI);
		this.ctx.stroke();
	}.bind(this));
	this.canvas.addEventListener('wheel', function(e) {
		e.preventDefault();
		console.log(e);
	});
};
