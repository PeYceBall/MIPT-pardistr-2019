#include <fstream>
#include <string>
#include <sstream>
#include <vector>

void map(std::stringstream& input_stream, std::vector<std::string>& output) {
  std::string word;

  while (input_stream >> word) {
    output.push_back(word + "\t1");
  }

  return;
}