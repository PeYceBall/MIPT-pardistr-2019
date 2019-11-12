#include <mpi.h>
#include <iostream>
#include <numeric>

int main(int argc, char** argv) {
  MPI_Init(NULL, NULL);

  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  int world_rank;

  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  if (world_rank == 0) {
    int N;
    std::cin >> N;
    int* a = new int[N];
    for (int i = 0; i < N; i++) {
      a[i] = i + 1;
    }

    int partial_sums[world_size];
    int batch_size = N / world_size;

    // divide a into almost equal parts
    for(int i = batch_size, j = 1; i < N; j++){
      // if the rest of a cannot be divided equally
      // make the next part one element bigger
      bool b = (N - i) % (world_size - j) != 0;
      MPI_Send(&a[i], batch_size + (int)b, MPI_INT, j, j, MPI_COMM_WORLD);

      i += (batch_size + (int)b);
    }

    partial_sums[0] = std::accumulate(a, a + batch_size, 0);

    for (int i = 1; i < world_size; i++) {
      MPI_Recv(&partial_sums[i], 1, MPI_INT, i, 1, MPI_COMM_WORLD,
               MPI_STATUS_IGNORE);

      std::cout << "Process " << i << " calculated sum of " << partial_sums[i]
                << std::endl;
    }

    std::cout << std::endl;
    int total_sum = std::accumulate(partial_sums, partial_sums + world_size, 0);
    std::cout << "Total sum is " << total_sum << std::endl;
    int real_sum = std::accumulate(a, a + N, 0);
    std::cout << "Real sum is " << real_sum << std::endl;
    std::cout << (real_sum == total_sum ? "Sums are equal"
                                        : "Sums are not equal")
              << std::endl;

    delete[] a;
  }

  if (world_rank != 0) {
    MPI_Status status;
    int buf_size;
    MPI_Probe(0, world_rank, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_INT, &buf_size);

    int* buf = new int[buf_size];
    MPI_Recv(buf, buf_size, MPI_INT, 0, world_rank, MPI_COMM_WORLD,
             MPI_STATUS_IGNORE);

    int sum = std::accumulate(buf, buf + buf_size, 0);
    MPI_Send(&sum, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);

    delete[] buf;
  }

  MPI_Finalize();
  return 0;
}