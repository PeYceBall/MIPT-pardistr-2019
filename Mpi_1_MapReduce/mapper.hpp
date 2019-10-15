#include <fstream>
#include <string>

void map(const char* input_path, const char* output_path) {
  std::ifstream input_file(input_path);
  std::string word;

  std::vector<std::string> output;

  while (input_file >> word) {
    output.push_back(word + "\t1");
  }

  std::ofstream output_file(output_path);
  for (int i = 0; i < output.size(); i++) {
    output_file << output[i] << std::endl;
  }

  input_file.close();
  output_file.close();
  return;
}