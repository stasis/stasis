#include <config.h>
#include <stasis/common.h>
#include <stasis/util/time.h>
#include <stasis/util/random.h>
#include <stdio.h>
#include <math.h>
#include <assert.h>


void * buf;
static void my_pread_help(int fd, long long off) {
  int err = pread(fd, buf, 512, off);
  if(err == -1) {
    perror("Could not read from file");
    abort();
  }
}

static void drop_buffers() {
  int fd = open("/proc/sys/vm/drop_caches", O_TRUNC|O_WRONLY);
  if(fd == -1) { perror("Couldn't drop page cache"); abort(); }
  char * str = "1\n";
  int err = write(fd, str, 3);
  if(err == -1) {
    perror("write to drop page cache failed");
    abort();
  }
  err = close(fd);
  if(err == -1) {
    perror("could not close file");
    abort();
  }
}

#define my_pread(start_off, off) my_pread_help(fd, start_off + off)

int main(int argc, char * argv[]) {
  if(argc != 7) {
    printf("Usage: %s filename steps iterations start_off length random_mode\n", argv[0]);
    abort();
  }
  #ifdef HAVE_POSIX_MEMALIGN
  posix_memalign(&buf, 512, 512);
  #else
  buf = malloc(2 * 512);
  buf = (void*)(((intptr_t)buf) & ~(512-1));
  #endif
  const char * filename = argv[1];
  int fd = open(filename, O_RDONLY);//|O_DIRECT);
  if(fd == -1) {
    perror("Could not open file");
    abort();
  }
  int steps = atoi(argv[2]);
  int iter  = atoi(argv[3]);
  long length = atoll(argv[4]);
  long start_off = atoll(argv[5]);
  long random_mode = atoi(argv[6]);

  double**  sum_x  = stasis_calloc(steps, double*);
  double**  sum_x2 = stasis_calloc(steps, double*);
  long** sample_count = stasis_calloc(steps, long*);

  for(int s = 0; s < steps; s++) {
    sum_x[s] = stasis_calloc(steps, double);
    sum_x2[s] = stasis_calloc(steps, double);
    sample_count[s] = stasis_calloc(steps, long);
  }

  long stride = length / steps;
  printf("filename = %s steps = %d iter = %d length = %ld start_off = %ld stride = %ld\n",
          filename,     steps,     iter,     length,      start_off,      stride);
  assert(stride);
  for(int iteration = 0; iteration < iter; iteration++) {
    if(!random_mode || random_mode == 2) {
      for(int xstep = 0; xstep < steps; xstep++) {
        for(int ystep = 0; ystep < steps; ystep++) {
          drop_buffers();

          // position head (do not count the time this takes)
          if(random_mode) {
            my_pread(start_off, xstep * stride + stasis_util_random64(stride));
          } else {
            my_pread(start_off, xstep * stride);

          }

          struct timeval start, stop;
          gettimeofday(&start, 0);
          if(random_mode) {
            my_pread(start_off, ystep * stride + stasis_util_random64(stride));
          } else {
            my_pread(start_off, ystep * stride);
          }
          gettimeofday(&stop, 0);

          double elapsed = stasis_timeval_to_double(
                             stasis_subtract_timeval(stop, start));
          sum_x [xstep][ystep]  += elapsed;
          sum_x2[xstep][ystep]  += elapsed*elapsed;
        }
        printf("%d", xstep % 10); fflush(stdout);
      }
    } else {
      for(long x = 0; x < steps * steps; x++) {
        long long start_pos = stasis_util_random64(length);
        long long stop_pos = stasis_util_random64(length);

        int xstep = start_pos / stride;
        int ystep = stop_pos / stride;

        drop_buffers();
        // position head
        my_pread(start_off, start_pos);

        struct timeval start, stop;
        gettimeofday(&start, 0);
        my_pread(start_off, stop_pos);
        gettimeofday(&stop, 0);

        double elapsed = stasis_timeval_to_double(
                           stasis_subtract_timeval(stop, start));
        sum_x [xstep][ystep]  += elapsed;
        sum_x2[xstep][ystep]  += elapsed*elapsed;
        sample_count[xstep][ystep] ++;

      }
    }
    printf("\nIteration %d mean seek time:\n", iteration);
    for(int xstep = 0; xstep < steps; xstep++) {
      for(int ystep = 0; ystep < steps; ystep++) {
        double n = (double) random_mode==1
            ?(sample_count[xstep][ystep]
               ?sample_count[xstep][ystep]
               :1)
            :(iteration+1);
        printf("%f%s", sum_x[xstep][ystep] / n,
                       (ystep==steps-1)?"\n":"\t");
      }
    }
    printf("\nIteration %d stddev seek time:\n", iteration);
    for(int xstep = 0; xstep < steps; xstep++) {
      for(int ystep = 0; ystep < steps; ystep++) {
        double n = (double) random_mode==1
            ?(sample_count[xstep][ystep]
               ?sample_count[xstep][ystep]
               :1)
            :(iteration+1);
        double mean = sum_x[xstep][ystep]/n;
        printf("%f%s", sqrt(sum_x2[xstep][ystep]/n - mean*mean),
                       (ystep==steps-1)?"\n":"\t");
      }
    }
    fflush(stdout);
  }
  close(fd);
  return 0;
}
