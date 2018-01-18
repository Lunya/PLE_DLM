let Point = function(x, y) {
	this.x = x || 0;
	this.y = y || 0;
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

	this.size = new Point(this.parent.clientWidth, this.parent.clientHeight);
	this.resizeCanvas();

	// bindings
	window.addEventListener('resize', function(e) {
		this.resizeCanvas();
		this.ctx.lineWidth = 5;
		this.ctx.strokeRect(0, 0, this.size.x, this.size.y);
	}.bind(this));
};
