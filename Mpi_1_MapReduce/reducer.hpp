#include <fstream>
#include <sstream>
#include <string>

// reduce has to read from file instead of
// receiving input directly since input can be to large to fit in memory
void reduce(const char* input_path, const char* output_path) {
  std::ifstream input_file(input_path);
  std::ofstream output_file(output_path);

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

  input_file.close();
  output_file.close();
}