const LATITUDE_MIN = -90.0;
const LATITUDE_MAX = 90.0;
const LONGITUDE_MIN = -180.0;
const LONGITUDE_MAX = 180.0;

let Point = function(x, y) {
	this.x = x || 0;
	this.y = y || 0;

	this.clone = () => {
		return new Point(this.x, this.y);
	};
	this.toString = () => {
		return '{Point} x:' + this.x.toString() + ' y:' + this.y.toString();
	};
	this.neg = () => {
		this.x = -this.x; this.y = -this.y; return this;
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
	this.divBy = (num) => {
		this.x /= num; this.y /= num; return this;
	};
	this.add = (point) => {
		this.x += point.x; this.y += point.y; return this;
	};
	this.sub = (point) => {
		this.x -= point.x; this.y -= point.y; return this;
	};
};

let MapImage = function(data, size, position, zoomLevel) {
	// private functions
	this.buildImage = function(gradient) {
		let canvas = document.createElement('canvas');
		let ctx = canvas.getContext('2d');
		canvas.width = this.size.x;
		canvas.height = this.size.y;
		let id = ctx.createImageData(this.size.x, this.size.y);
		let data = id.data;
		for (let i = 0, size = this.data.length; i < size; i++) {
			let I = i * 4;
			const color = gradient.getColorAtValue((this.data[i]*0.122 + 8001)/16000);
			data[I] = color.r * 255;
			data[I+1] = color.g * 255;
			data[I+2] = color.b * 255;
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

	this.isVisible = function(topLeft, bottomRight) {
		const BR = this.position.clone().add(this._size);
		return (this.position.x <= bottomRight.x) && (this.position.y <= bottomRight.y)
			&& (BR.x >= topLeft.x) && (BR.y >= topLeft.y);
	};

	// constructor
	this.data = data; // Bitmap
	this.size = size; // Point
	this.position = position; // Point
	this.zoomLevel = zoomLevel;
	this._image = new Image();
	this._size = this.size.clone().mulBy((256 * 2**this.zoomLevel)/180); // image size in degrees
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
		const boundaries = this.getBoundary();
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

		// sort images by level
		this.images.sort((a, b) => a.zoomLevel < a.zoomLevel)
			.filter((i) => i.zoomLevel >= boundaries[2])
			.filter((i) => i.isVisible(boundaries[0], boundaries[1]))
			.map(function (image) {
				const position = image.position.clone().add(new Point(180, 90)).mulBy(this.zoom).add(mapPosition);
				const size = image.size.clone().mulBy((180 / (2**image.zoomLevel)) / 256).mulBy(this.zoom);
				this.ctx.drawImage(image.getImage(),
					position.x, position.y,
					size.x, size.y);
		}.bind(this));
		// draw images from level 0 to level max
		/*for (let i = 0; i < this.images.length; ++i) {
			const image = this.images[i];
			const position = image.position.clone().add(new Point(180, 90)).mulBy(this.zoom).add(mapPosition);
			const size = image.size.clone().mulBy((180 / (2**image.zoomLevel)) / 256).mulBy(this.zoom);
			this.ctx.drawImage(image.getImage(),
				position.x, position.y,
				size.x, size.y);
		}*/

		this.write(new Point(20, 50), this.lastZoom.toString() + ' -> ' + this.zoom.toString());
		this.write(new Point(20, 80), boundaries[2]);

		// draw mouse position
		this.ctx.beginPath();
		this.ctx.arc(
			this.mousePos.x, this.mousePos.y,
			5, 0, 2*Math.PI);
		this.ctx.stroke();
	};

	this.getData = function() {
		const boundaries = this.getBoundary();
		// get image position to query
		ajax({
			url: '/hbase/stats', method: 'POST', type: 'arraybuffer',
			data: {
				lngMin: boundaries[0].x,
				latMin: boundaries[0].y,
				lngMax: boundaries[1].x,
				latMax: boundaries[1].y,
				zoom: boundaries[2]}
		}).then(function(data) {
			// query these images and add them to the list
			for (let i = 0; i < data.length; ++i) {
				const imgData = data[i];
				ajax({
					url: '/hbase/heights', method: 'POST', type: 'arraybuffer',
					data: {
						position: imgData.position,
						zoom: imgData.zoom
					}
				}).then(function(data) {
					let image = new MapImage(
						new Uint16Array(data), new Point(256, 256),
						new Point(imgData.lng, imgData.lat), boundaries[2]);
					image.buildImage(this.gradient);
					this.images.push(image);
					this.images = this.images
						.sort((a, b) => a._lastAccess < b._lastAccess)
						.slice(0, this.maxImages);
				}.bind(this));
			}
		}.bind(this));
	};

	// public functions

	this.write = function(position, text, size) {
		this.ctx.font = (size || 12) + 'px monospace';
		this.ctx.fillText(text, position.x, position.y);
	};

	this.getBoundary = function() {
		const recenter = new Point(180, 90);
		const topLeft = this.mapPosition.clone().neg().divBy(this.zoom).sub(recenter);
		const bottomRight = this.mapPosition.clone().neg().add(this.size).divBy(this.zoom).sub(recenter);
		let zoomLevel = 0;
		const pixelRatio = bottomRight.clone().sub(topLeft).div(this.size);
		while ((180 / (256 * 2 ** zoomLevel)) > pixelRatio.x)
			++zoomLevel;
		return [topLeft, bottomRight, zoomLevel];
	};

	this.resetView = function() {
		this.zoom = 1;
		this.update();
	};

	// constructor
	this.parent = parent;

	this.canvas = document.createElement('canvas');
	this.ctx = this.canvas.getContext('2d');
	this.canvas.classList.add('map');
	this.parent.appendChild(this.canvas);

	this.images = [];
	this.maxImages = 100;
	this.gradient = new ColorGradient();
	this.gradient.createGPS();

	this.lastZoom = this.zoom = 1.0; // zoom = pixel * degree
	this.minZoom = 0.5;
	this.maxZoom = 12;
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
		if (e.buttons === 1) {
			this.mapPosition.add(this.mousePos).sub(this.lastMousePos);
		}
		this.update();
	}.bind(this));
	this.canvas.addEventListener('wheel', function(e) {
		e.preventDefault();
		/*console.log(e.deltaY,
			e.clientX - e.target.offsetLeft,
			e.clientY - e.target.offsetTop);*/
		this.lastZoom = this.zoom;
		this.zoom = (e.deltaY < 0) ? this.zoom * 1.5 : this.zoom * (1/1.5);
		this.zoom = Math.max(Math.min(this.zoom, this.maxZoom), this.minZoom);
		const lastMapSize = this.mapSize.clone().mulBy(this.lastZoom);
		const currMapSize = this.mapSize.clone().mulBy(this.zoom);
		const fraction = this.mapPosition.clone().sub(this.mousePos).div(lastMapSize); // position of pointer on map (0 on top left, 1 on bottom right)
		const offset = currMapSize.sub(lastMapSize).mul(fraction);
		this.mapPosition.add(offset);
		this.update();
	}.bind(this));
};
