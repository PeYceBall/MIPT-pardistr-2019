#include <fstream>
#include <sstream>
#include <string>

void reduce(std::fstream& input_file, std::ofstream& output_file) {
  std::string line, key;
  int value;
  while (std::getline(input_file, line)) {
    std::istringstream iss(line);
    if (!(iss >> key)) {
      continue;
    }

    int summ = 0;
    while (iss >> value) {
      summ += value;
    }

    output_file << key << "\t" << summ << std::endl;
  }

}