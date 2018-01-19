import  sys

if __name__ == '__main__':
	dataSize = 2 # bytes
	imageSize = 256 # pixels
	levels = int(sys.argv[1]) if len(sys.argv) >= 2 else 1

	size = 0
	'''
	\sum_{i=0}^{n} t \times 256^{2} \times 4^{i}
	With n the number of levels and t the datatype size
	'''
	for n in range(levels):
		tmp = 2 * (imageSize ** 2) * (4 ** n)
		size += tmp

	print '''Estimated occupied size for parameters:
data size: %d
image size: %d
levels: %d
size: %d o
size: %f Go''' % (dataSize, imageSize, levels, size, float(size) / (1024.0**3.0))
