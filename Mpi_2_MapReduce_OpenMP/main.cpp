#include <dirent.h>
#include <mpi.h>
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <sstream>
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
  double start_time = omp_get_wtime();

  std::vector<std::ofstream> output_files(num_reducers);
  for (int i = 0; i < output_files.size(); i++) {
    char temp[200];
    // don't want different mappers to overwrite same file
    sprintf(temp, "%s/%d/%d_%d", output_dir, i, mapper, map_job);
    output_files[i].open(temp);
  }

  std::string line;
  std::vector<std::string> lines;
  while (std::getline(input_file, line)) {
    lines.push_back(line);
  }

  int i;
#pragma omp parallel default(shared) private(i)
  {
    std::string key, value;
    std::hash<std::string> hash_func;
    std::vector<std::vector<std::string>> to_write(num_reducers);

#pragma omp for schedule(static)
    for (i = 0; i < lines.size(); i++) {
      std::istringstream iss(lines[i]);
      // assume map produces output of "key \t value"
      iss >> key >> value;
      int reducer = hash_func(key) % num_reducers;
      std::string temp = key + "\t" + value + "\n";
      to_write[reducer].push_back(temp);
    }

// data race prevention
#pragma omp critical
    {
      for (int reducer = 0; reducer < to_write.size(); reducer++) {
        for (int j = 0; j < to_write[reducer].size(); j++) {
          output_files[reducer] << to_write[reducer][j];
        }
      }
    }
  }

  for (int i = 0; i < output_files.size(); i++) {
    output_files[i].close();
  }

  input_file.close();

  double end_time = omp_get_wtime();
}

// merge all map outputs with same keys from different files
// and sort them by key
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

// executed by each mapper
void map_routine(std::fstream& map_fin, int world_rank, int i,
                 int num_reducers) {
  char map_output_path[100];
  sprintf(map_output_path, "map outputs/%d_%d", world_rank, i);

  std::ofstream map_fout(map_output_path);

  map_fin.seekg(0, std::ios::end);
  int size = map_fin.tellg();

  int pos;
#pragma omp parallel default(shared) private(pos) if (size > 1000)
  {
    int num_threads = omp_get_num_threads();
    int chunk_size = size / num_threads;

#pragma omp for schedule(static)
    for (pos = 0; pos < size; pos += chunk_size) {
      std::string buffer(chunk_size, ' ');
#pragma omp critical
      {
        map_fin.seekg(pos);
        map_fin.read(&buffer[0], chunk_size);
      }

      std::stringstream map_input_stream(buffer);
      std::vector<std::string> map_output;

      map(map_input_stream, map_output);

#pragma omp critical
      {
        for (int i = 0; i < map_output.size(); i++) {
          map_fout << map_output[i] << std::endl;
        }
      }
    }
  }

  map_fin.close();
  map_fout.close();

  // shuffle result of map to reducers
  std::fstream shuffle_fin(map_output_path);
  shuffle(shuffle_fin, world_rank, i, num_reducers, "intermediate");
}

// executed by each reducer
void reduce_routine(std::fstream& reduce_fin, std::ofstream& reduce_fout) {
  reduce_fin.seekg(0, std::ios::end);
  int size = reduce_fin.tellg();

  int pos;
#pragma omp parallel default(shared) private(pos) if (size > 1000)
  {
    // splits reduce input file by bytes.
    // Possible loss of data consistency but faster
    // than splitting by lines.
    int num_threads = omp_get_num_threads();
    int chunk_size = size / num_threads;

#pragma omp for schedule(static)
    for (pos = 0; pos < size; pos += chunk_size) {
      std::string buffer(chunk_size, ' ');
#pragma omp critical
      {
        reduce_fin.seekg(pos);
        reduce_fin.read(&buffer[0], chunk_size);
      }

      std::stringstream reduce_input_stream(buffer);
      std::vector<std::string> reduce_output;

      reduce(reduce_input_stream, reduce_output);

#pragma omp critical
      {
        for (int i = 0; i < reduce_output.size(); i++) {
          reduce_fout << reduce_output[i] << std::endl;
        }
      }
    }
  }
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

  // probably should be done using groups
  // 0 -- master, 1..num_reducers -- reducers, num_reducers+1..world_size-1 --
  // mappers
  if (world_rank == 0) {
    // master routine

    double start_time = omp_get_wtime();

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
      MPI_Request_free(&request);
    }

    // send stop signs to all mappers
    for (int i = num_reducers + 1; i < world_size; i++) {
      char stop_sign = '\0';
      MPI_Request request;
      MPI_Isend(&stop_sign, 1, MPI_CHAR, i, 0, MPI_COMM_WORLD, &request);
      MPI_Request_free(&request);
    }

    // get responses from mappers
    for (int i = num_reducers + 1; i < world_size; i++) {
      wait(i);
    }

    // notify reducers that all mappers finished
    for (int i = 1; i <= num_reducers; i++) {
      notify(i);
    }

    // wait for all reducers to finish(since we want to measure the time)
    for (int i = 1; i <= num_reducers; i++) {
      wait(i);
    }

    double end_time = omp_get_wtime();

    std::cout << "Time: " << end_time - start_time << std::endl;

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

    std::fstream reduce_fin(reduce_input_path);
    std::ofstream reduce_fout(reduce_output_path);

    reduce_routine(reduce_fin, reduce_fout);

    reduce_fin.close();
    reduce_fout.close();

    // notify master that job is done
    notify(0);
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

    // map and shuffle
    for (int i = 0; i < map_input_paths.size(); i++) {
      std::fstream map_fin(map_input_paths[i]);
      map_routine(map_fin, world_rank, i, num_reducers);
    }

    // notify Master that job is done
    notify(0);
  }

  MPI_Finalize();
  return 0;
}