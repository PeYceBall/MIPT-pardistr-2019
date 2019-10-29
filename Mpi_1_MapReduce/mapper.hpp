#include <fstream>
#include <string>

void map(std::fstream& input_file, std::ofstream& output_file) {
  std::string word;

  std::vector<std::string> output;

  while (input_file >> word) {
    output.push_back(word + "\t1");
  }

  for (int i = 0; i < output.size(); i++) {
    output_file << output[i] << std::endl;
  }

  return;
}