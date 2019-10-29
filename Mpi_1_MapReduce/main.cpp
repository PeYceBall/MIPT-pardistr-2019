#include <dirent.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "mapper.hpp"
#include "reducer.hpp"

// get list of all file names in directory
std::vector<std::string> files_in_dir(const char* path) {
  std::vector<std::string> result;
  DIR* dir;
  struct dirent* ent;

  if ((dir = opendir(path)) != NULL) {
    while ((ent = readdir(dir)) != NULL) {
      if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) {
        continue;
      }

      char temp[100];
      sprintf(temp, "%s/%s", path, ent->d_name);
      result.push_back(temp);
    }
    closedir(dir);
  } else {
    std::cout << "failed to read from input directory\n";
  }

  return result;
}

// Process output of particular map and write (key, value)
// to corresponding reducer's directory
void shuffle(std::fstream& input_file, int mapper, int map_job,
             int num_reducers, const char* output_dir) {
  std::vector<std::ofstream> output_files(num_reducers);
  for (int i = 0; i < output_files.size(); i++) {
    char temp[200];
    // don't want different mappers to overwrite same file
    sprintf(temp, "%s/%d/%d_%d", output_dir, i, mapper, map_job);
    output_files[i].open(temp);
  }

  std::string key, value;
  std::hash<std::string> hash_func;
  // assume map produces output of "key \t value"
  while (input_file >> key >> value) {
    int reducer = hash_func(key) % num_reducers;
    output_files[reducer] << key << "\t" << value << std::endl;
  }

  for (int i = 0; i < output_files.size(); i++) {
    output_files[i].close();
  }

  input_file.close();
  return;
}

// merge all map outputs with same keys from different files and sort them by
// key
void sort(const char* input_dir, const char* output_path) {
  std::ofstream output_file(output_path);

  std::map<std::string, std::vector<std::string>> map;
  std::string key, value;
  std::vector<std::string> input_files = files_in_dir(input_dir);

  for (int i = 0; i < input_files.size(); i++) {
    std::ifstream input_file(input_files[i]);
    while (input_file >> key >> value) {
      if (map.find(key) == map.end()) {
        std::vector<std::string> temp;
        temp.push_back(value);
        map.insert({key, temp});
      } else {
        map.find(key)->second.push_back(value);
      }
    }

    input_file.close();
  }

  // write "key  value1  value2  ..." to file
  for (auto it = map.begin(); it != map.end(); it++) {
    output_file << it->first << "\t";
    for (auto v : it->second) {
      output_file << v << "\t";
    }

    output_file << std::endl;
  }

  output_file.close();
}

// wait until sender process sends notify
void wait(int sender) {
  int response;
  MPI_Recv(&response, 1, MPI_INT, sender, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

// notify receiver process waiting for this process
void notify(int receiver) {
  int msg = 0;
  MPI_Send(&msg, 1, MPI_INT, receiver, 0, MPI_COMM_WORLD);
}

// argv[1] -- input directory (not used)
// argv[2] -- output directory
// argv[3] -- number of reducers(R)
int main(int argc, char** argv) {
  MPI_Init(&argc, &argv);

  int num_reducers = atoi(argv[3]);

  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  int world_rank;

  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  if (world_rank == 0) {
    // master routine

    // get names of all files in input directory
    const char* map_inputs_path = "map inputs";

    std::vector<std::string> input_files = files_in_dir(map_inputs_path);

    // send file names to mappers while there are some left
    for (int i = num_reducers + 1, j = 0; j < input_files.size(); i++, j++) {
      if (i >= world_size) {
        i = num_reducers + 1;
      }

      int len = input_files[j].size();
      MPI_Request request;
      MPI_Isend(input_files[j].c_str(), len + 1, MPI_CHAR, i, 0, MPI_COMM_WORLD,
                &request);
    }

    // send stop signs to all mappers
    for (int i = num_reducers + 1; i < world_size; i++) {
      char stop_sign = '\0';
      MPI_Request request;
      MPI_Isend(&stop_sign, 1, MPI_CHAR, i, 0, MPI_COMM_WORLD, &request);
    }

    // get responses from mappers
    for (int i = num_reducers + 1; i < world_size; i++) {
      wait(i);
    }

    // notify reducers that all mappers finished
    for (int i = 1; i <= num_reducers; i++) {
      notify(i);
    }

  } else if (world_rank <= num_reducers) {
    // reducer routine

    // wait for all mappers to finish
    wait(0);

    // sort data by keys, join values with the same key
    char sort_input_dir[100], sort_output_path[100];
    sprintf(sort_input_dir, "intermediate/%d", world_rank - 1);
    sprintf(sort_output_path, "reduce inputs/%d", world_rank - 1);

    sort(sort_input_dir, sort_output_path);

    // reduce
    char reduce_input_path[100], reduce_output_path[100];
    strcpy(reduce_input_path, sort_output_path);
    sprintf(reduce_output_path, "%s/%d", argv[2], world_rank - 1);

    std::fstream reduce_input_file(reduce_input_path);
    std::ofstream reduce_output_file(reduce_output_path);

    reduce(reduce_input_file, reduce_output_file);

    reduce_input_file.close();
    reduce_output_file.close();
  } else {
    // mapper routine

    // get paths from Master until got "" - stop sign
    std::vector<std::string> map_input_paths;
    while (true) {
      char map_input_path[100];
      MPI_Request request;
      MPI_Irecv(map_input_path, 100, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &request);
      MPI_Status status;
      MPI_Wait(&request, &status);

      if (strlen(map_input_path) == 0) {
        break;
      }

      map_input_paths.push_back(map_input_path);
    }

    // map
    for (int i = 0; i < map_input_paths.size(); i++) {
      char map_output_path[100];
      sprintf(map_output_path, "map outputs/%d_%d", world_rank, i);

      std::fstream map_input_file(map_input_paths[i]);
      std::ofstream map_output_file(map_output_path);

      map(map_input_file, map_output_file);

      map_input_file.close();
      map_output_file.close();

      // shuffle result of map to reducers
      std::fstream shuffle_input_file(map_output_path);
      shuffle(shuffle_input_file, world_rank, i, num_reducers, "intermediate");
    }

    // notify Master that job is done
    notify(0);
  }

  MPI_Finalize();
  return 0;
}