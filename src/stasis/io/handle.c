/*
 * handle.c
 *
 *  Created on: May 7, 2009
 *      Author: sears
 */
#include <config.h>

#include <stasis/common.h>
#include <stasis/constants.h>
#include <stasis/flags.h>
#include <stasis/io/handle.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <assert.h>
#include <stdio.h>

stasis_handle_t* stasis_handle_default_factory() {
  return stasis_handle_file_factory(stasis_store_file_name, O_CREAT | O_RDWR | stasis_buffer_manager_io_handle_flags, FILE_PERM);
}
