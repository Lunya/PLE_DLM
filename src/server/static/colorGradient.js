// Strongly inspired by : http://www.andrewnoske.com/wiki/Code_-_heatmaps_and_color_gradients
let Color = function(r, g, b) {
	// private functions
	// public functions
	// constructor
	this.r = r || 0.0;
	this.g = g || 0.0;
	this.b = b || 0.0;
};

let ColorGradient = function() {
	// private functions
	// public functions

	this.addColorPoint = function(colorPoint) {
		//const index = this.colorPoints.findIndex((e) => e.val < colorPoint.val);
		// TODO: add to good place
		this.colorPoints.push(colorPoint);
		this.colorPoints.sort((a, b) => a.val > b.val);
	};

	/* object of the form :
		{
			name: "NameOfGradient",
			values: {
				stopPoint: [red, green, blue],
				...
			}
		}
		It will be further accessed by: "create('NameOfGradient')".
	*/
	this.fromObject = function(object) {
		this.knownGradients[object.name] = Object.keys(object.values).map((key) => {
			const val = object.values[key];
			return new ColorGradient.ColorPoint(key, val[0], val[1], val[2]);
		});
		this.knownGradients[object.name].sort((a, b) => a.val > b.val);
		this.create(object.name);
	};

	this.create = function(name) {
		if (Object.keys(this.knownGradients).indexOf(name) !== -1)
			this.colorPoints = this.knownGradients[name];
		else
			this.clearGradient();
	};

	this.clearGradient = function() {
		this.colorPoints = [];
	};

	this.getColorAtValue = function(value) {
		if (this.colorPoints.length === 0)
			return new Color();
		for (let i = 0; i < this.colorPoints.length; ++i) {
			const currC = this.colorPoints[i];
			if (value < currC.val) {
				const prevC = this.colorPoints[Math.max(0, i-1)];
				const valueDiff = prevC.val - currC.val;
				const fractBetween = (valueDiff === 0.0) ? 0.0 : (value - currC.val) / valueDiff;
				return new Color(
					(prevC.r - currC.r) * fractBetween + currC.r,
					(prevC.g - currC.g) * fractBetween + currC.g,
					(prevC.b - currC.b) * fractBetween + currC.b
				);
			}
		}
		const last = this.colorPoints[this.colorPoints.length-1];
		return new Color(last.r, last.g, last.b);
	};
	// constructor
	this.colorPoints = [];
	this.knownGradients = {};
};

ColorGradient.ColorPoint = function(val, r, g, b) {
	this.val = val;
	this.r = r || 0.0;
	this.g = g || 0.0;
	this.b = b || 0.0;
};


ColorGradient.prototype.createRainbow = function() {
	this.clearGradient();
	this.addColorPoint(new ColorGradient.ColorPoint(0.0, 1.0, 0.0, 0.0));
	this.addColorPoint(new ColorGradient.ColorPoint(.25, 1.0, 1.0, 0.0));
	this.addColorPoint(new ColorGradient.ColorPoint(0.5, 0.0, 1.0, 0.0));
	this.addColorPoint(new ColorGradient.ColorPoint(.75, 0.0, 1.0, 1.0));
	this.addColorPoint(new ColorGradient.ColorPoint(1.0, 0.0, 0.0, 1.0));
};

ColorGradient.prototype.createHotmap = function() {
	this.clearGradient();
	this.addColorPoint(new ColorGradient.ColorPoint(0.0, 0.0, 0.0, 0.0));
	this.addColorPoint(new ColorGradient.ColorPoint(0.333, 1.0, 0.0, 0.0));
	this.addColorPoint(new ColorGradient.ColorPoint(0.667, 1.0, 1.0, 0.0));
	this.addColorPoint(new ColorGradient.ColorPoint(1.0, 1.0, 1.0, 1.0));
};

ColorGradient.prototype.createGPS = function() {
	this.clearGradient();
	this.addColorPoint(new ColorGradient.ColorPoint(0.000/100,   0/255,  0/255,   0/255));
	this.addColorPoint(new ColorGradient.ColorPoint(6.250/100,   0/255,  5/255,  25/255));
	this.addColorPoint(new ColorGradient.ColorPoint(12.500/100,   0/255, 10/255,  50/255));
	this.addColorPoint(new ColorGradient.ColorPoint(18.750/100,   0/255, 80/255, 125/255));
	this.addColorPoint(new ColorGradient.ColorPoint(25.000/100,   0/255,150/255, 200/255));
	this.addColorPoint(new ColorGradient.ColorPoint(31.250/100,  86/255,197/255, 184/255));
	this.addColorPoint(new ColorGradient.ColorPoint(37.500/100, 172/255,245/255, 168/255));
	this.addColorPoint(new ColorGradient.ColorPoint(43.750/100, 211/255,250/255, 211/255));
	this.addColorPoint(new ColorGradient.ColorPoint(50.000/100, 250/255,255/255, 255/255));
	this.addColorPoint(new ColorGradient.ColorPoint(50.000/100,  70/255,120/255,  50/255));
	this.addColorPoint(new ColorGradient.ColorPoint(53.120/100, 120/255,100/255,  50/255));
	this.addColorPoint(new ColorGradient.ColorPoint(56.250/100, 146/255,126/255,  60/255));
	this.addColorPoint(new ColorGradient.ColorPoint(62.500/100, 198/255,178/255,  80/255));
	this.addColorPoint(new ColorGradient.ColorPoint(68.750/100, 250/255,230/255, 100/255));
	this.addColorPoint(new ColorGradient.ColorPoint(75.000/100, 250/255,234/255, 126/255));
	this.addColorPoint(new ColorGradient.ColorPoint(81.250/100, 252/255,238/255, 152/255));
	this.addColorPoint(new ColorGradient.ColorPoint(87.500/100, 252/255,243/255, 177/255));
	this.addColorPoint(new ColorGradient.ColorPoint(93.750/100, 253/255,249/255, 216/255));
	this.addColorPoint(new ColorGradient.ColorPoint(100.000/100, 255/255,255/255, 255/255));
};
