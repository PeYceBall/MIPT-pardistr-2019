#include <fstream>
#include <sstream>
#include <string>
#include <vector>

void reduce(std::stringstream& input_stream, std::vector<std::string>& output) {
  std::string line, key;
  int value;
  while (std::getline(input_stream, line)) {
    std::istringstream iss(line);
    if (!(iss >> key)) {
      continue;
    }

    int summ = 0;
    while (iss >> value) {
      summ += value;
    }

    char temp[500];
    sprintf(temp, "%s\t%d", key.c_str(), summ);
    output.push_back(temp);
  }

}