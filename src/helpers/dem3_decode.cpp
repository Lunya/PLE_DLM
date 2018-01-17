#include <iostream>
#include <fstream>
#include <string>
#include <iomanip>

using namespace std;

int main(int argc, const char * argv[])
{
	std::ifstream::pos_type size;

	//"N45W066.hgt"
	std::ifstream file (argv[1], std::ios::in|std::ios::binary|std::ios::ate);
	const int srtm_ver = 1201;
	int height[1201][1201];
	if (file.is_open())
	{
		file.seekg(0, std::ios::beg);
		unsigned char buffer[2];
		string filen = argv[1];
		filen = filen.substr(filen.size()-11,11);
		double lat = stod(filen.substr(1,2));
		double lng = stod(filen.substr(4,3));
		if (filen[0]=='S' || filen[0]=='s') lat *= -1;
		if (filen[3]=='W' || filen[3]=='w') lng *= -1;
		for (int i = 0; i<srtm_ver; ++i){
			for (int j = 0; j < srtm_ver; ++j) {
				if(!file.read( reinterpret_cast<char*>(buffer), sizeof(buffer) )) {
					std::cout << "Error reading file!" << std::endl;
					return -1;
				}
				height[i][j] = (buffer[0] << 8) | buffer[1];
				std::cout << std::setprecision(10) << lat + double(i) * 1. / double(srtm_ver) << "," << lng + double(j) * .001 / double(srtm_ver) << ","  << height[i][j]<< endl;
			}
		}
	}
	else
		return -1;

return 0;
}
