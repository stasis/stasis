/*
 * histogram.c
 *
 *  Created on: Sep 29, 2010
 *      Author: sears
 */
#include <stasis/util/histogram.h>

int stasis_auto_histogram_count = 0;
char ** stasis_auto_histogram_names = 0;
stasis_histogram_64_t ** stasis_auto_histograms = 0;

void stasis_histograms_auto_dump(void) {
  for(int i = 0; i < stasis_auto_histogram_count; i++) {
    printf("Histogram: %s\n", stasis_auto_histogram_names[i]);
    stasis_histogram_pretty_print_64(stasis_auto_histograms[i]);
  }
}
