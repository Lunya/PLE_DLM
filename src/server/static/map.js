const LATITUDE_MIN = -90.0;
const LATITUDE_MAX = 90.0;
const LONGITUDE_MIN = -180.0;
const LONGITUDE_MAX = 180.0;

let Point = function(x, y) {
	this.x = x || 0;
	this.y = y || 0;

	this.toString = () => {
		return '<Point> x:' + this.x.toString() + ' y:' + this.y.toString();
	};
	this.clone = () => {
		return new Point(this.x, this.y);
	};
	this.mul = (point) => {
		this.x *= point.x; this.y *= point.y; return this;
	};
	this.div = (point) => {
		this.x /= point.x; this.y /= point.y; return this;
	};
	this.mulBy = (num) => {
		this.x *= num; this.y *= num; return this;
	};
	this.add = (point) => {
		this.x += point.x; this.y += point.y; return this;
	};
	this.sub = (point) => {
		this.x -= point.x; this.y -= point.y; return this;
	};
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
		ctx.putImageData(id, 0, 0);
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
};

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
		// reset map
		this.ctx.clearRect(0, 0, this.size.x, this.size.y);

		for (let i = 0; i < this.images.length; i++) {
			let image = this.images[i];
			this.ctx.drawImage(image.getImage(), image.position.x, image.position.y);
		}
		// draw meridians
		// draw map boundaries
		const mapSize = this.mapSize.clone().mulBy(this.zoom);
		const mapPosition = this.mapPosition.clone();
		this.ctx.rect(
			mapPosition.x, mapPosition.y,
			mapSize.x, mapSize.y);
		this.ctx.strokeStyle = '#0F8';
		this.ctx.lineWidth = 3;
		this.ctx.stroke();

		for (let i = 0; i < this.images.length; ++i) {
			const image = this.images[i];
			const position = image.position.clone().mulBy(this.zoom).add(mapPosition);
			const size = image.size.clone().mulBy((180 / (2**0)) / 256).mulBy(this.zoom);
			console.log('Image drawed to ', position.toString());
			this.ctx.drawImage(image.getImage(),
				position.x, position.y,
				size.x, size.y);
		}

		this.write(new Point(20, 50), this.lastZoom.toString() + ' -> ' + this.zoom.toString());

		// draw mouse position
		this.ctx.beginPath();
		this.ctx.arc(
			this.mousePos.x, this.mousePos.y,
			5, 0, 2*Math.PI);
		this.ctx.stroke();
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

	this.lastZoom = this.zoom = 1.0; // zoom = pixel * degree
	this.size = new Point(this.parent.clientWidth, this.parent.clientHeight);
	this.mapPosition = new Point(0, 0); // position top left corner
	this.mapSize = new Point(360, 180);
	this.mousePos = new Point(0, 0);
	this.lastMousePos = new Point(0, 0);
	this.resizeCanvas();

	// bindings
	window.addEventListener('resize', function(e) {
		this.resizeCanvas().bind(this);
	}.bind(this));
	this.canvas.addEventListener('mousemove', function(e) {
		this.lastMousePos = this.mousePos.clone();
		this.mousePos.x = e.clientX - e.target.offsetLeft;
		this.mousePos.y = e.clientY - e.target.offsetTop;
		this.update();
	}.bind(this));
	this.canvas.addEventListener('wheel', function(e) {
		e.preventDefault();
		/*console.log(e.deltaY,
			e.clientX - e.target.offsetLeft,
			e.clientY - e.target.offsetTop);*/
		this.lastZoom = this.zoom;
		this.zoom = (e.deltaY < 0) ? this.zoom * 1.5 : this.zoom * (1/1.5);
		const lastMapSize = this.mapSize.clone().mulBy(this.lastZoom);
		const currMapSize = this.mapSize.clone().mulBy(this.zoom);
		const fraction = this.mapPosition.clone().sub(this.mousePos).div(lastMapSize); // position of pointer on map (0 on top left, 1 on bottom right)
		const offset = currMapSize.sub(lastMapSize).mul(fraction);
		this.mapPosition.add(offset);
		this.update();
	}.bind(this));
};
