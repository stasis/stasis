/*---
This software is copyrighted by the Regents of the University of
California, and other parties. The following terms apply to all files
associated with the software unless explicitly disclaimed in
individual files.

The authors hereby grant permission to use, copy, modify, distribute,
and license this software and its documentation for any purpose,
provided that existing copyright notices are retained in all copies
and that this notice is included verbatim in any distributions. No
written agreement, license, or royalty fee is required for any of the
authorized uses. Modifications to this software may be copyrighted by
their authors and need not follow the licensing terms described here,
provided that the new terms are clearly indicated on the first page of
each file where they apply.

IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY
FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
ARISING OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY
DERIVATIVES THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
NON-INFRINGEMENT. THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, AND
THE AUTHORS AND DISTRIBUTORS HAVE NO OBLIGATION TO PROVIDE
MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

GOVERNMENT USE: If you are acquiring this software on behalf of the
U.S. government, the Government shall have only "Restricted Rights" in
the software and related documentation as defined in the Federal
Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2). If you are
acquiring the software on behalf of the Department of Defense, the
software shall be classified as "Commercial Computer Software" and the
Government shall have only "Restricted Rights" as defined in Clause
252.227-7013 (c) (1) of DFARs. Notwithstanding the foregoing, the
authors grant the U.S. Government and others acting in its behalf
permission to use and distribute the software in accordance with the
terms specified in this license.
---*/
/**
 * @file
 *
 * defines various constants
 *
 * @todo Sometime, LLADD's #includes need to be cleaned up.  In
 * particular, we should make sure everything directly or indirectly
 * includes this file, common.h, and constants.h 
 *
 * @ingroup LLADD_CORE
 *
 * $Id$
 */

#ifndef __CONSTANTS_H__
#define __CONSTANTS_H__
#include <lladd/transactional.h>
/*#define DEBUG 1*/

#define LOG_FILE "logfile.txt"
#define LOG_FILE_SCRATCH "logfile.txt~"
#define STORE_FILE "storefile.txt"
#define BLOB0_FILE "blob0_file.txt"
#define BLOB1_FILE "blob1_file.txt"

/* 
   define error codes
 */
#define LLADD_DEADLOCK       -1
#define LLADD_NO_MEM         -2
#define LLADD_IO_ERROR       -3
#define LLADD_INTERNAL_ERROR -4

//#define FILE_OPRN_ERROR 2
//#define FILE_READ_ERROR 3
//#define FILE_WRITE_ERROR 4
//#define FILE_WRITE_OPEN_ERROR 5
//#define MEM_WRITE_ERROR 6

#define PAGE_SIZE 4096

//#define MAX_BUFFER_SIZE 100003 
/*#define MAX_BUFFER_SIZE 20029 */
//#define MAX_BUFFER_SIZE 10007 
//#define MAX_BUFFER_SIZE 5003
#define MAX_BUFFER_SIZE 2003 
//#define MAX_BUFFER_SIZE 4006 
/* #define MAX_BUFFER_SIZE 71  */
/*#define MAX_BUFFER_SIZE 7 */

#define MAX_TRANSACTIONS 1000

/** Operation types */

#define NO_INVERSE_WHOLE_PAGE -2
#define NO_INVERSE            -1
#define OPERATION_SET          0
#define OPERATION_INCREMENT    1
#define OPERATION_DECREMENT    2
#define OPERATION_ALLOC        3
#define OPERATION_PREPARE      4
#define OPERATION_LHINSERT     5
#define OPERATION_LHREMOVE     6
#define OPERATION_DEALLOC      7
#define OPERATION_REALLOC      8
/*#define OPERATION_PAGE_ALLOC   ?
  #define OPERATION_PAGE_DEALLOC 9 */
#define OPERATION_PAGE_SET     10
#define OPERATION_UPDATE_FREESPACE 11
#define OPERATION_UPDATE_FREESPACE_INVERSE 12
#define OPERATION_UPDATE_FREELIST 13
#define OPERATION_UPDATE_FREELIST_INVERSE 14
#define OPERATION_FREE_PAGE   15
#define OPERATION_ALLOC_FREED 16
#define OPERATION_UNALLOC_FREED 17
#define OPERATION_NOOP        18
#define OPERATION_INSTANT_SET 19
#define OPERATION_ARRAY_LIST_ALLOC 20
#define OPERATION_INITIALIZE_FIXED_PAGE 21
#define OPERATION_UNINITIALIZE_PAGE 22
#define OPERATION_LINEAR_INSERT 23
#define OPERATION_UNDO_LINEAR_INSERT 24
#define OPERATION_LINEAR_DELETE 25
#define OPERATION_UNDO_LINEAR_DELETE 26
#define OPERATION_SET_RANGE 27
#define OPERATION_SET_RANGE_INVERSE 28
#define OPERATION_LINKED_LIST_INSERT 29
#define OPERATION_LINKED_LIST_REMOVE 30
#define OPERATION_LINEAR_HASH_INSERT 31
#define OPERATION_LINEAR_HASH_REMOVE 32

#define OPERATION_SET_RAW    33
#define OPERATION_INSTANT_SET_RAW 34

#define OPERATION_ALLOC_BOUNDARY_TAG 35

#define OPERATION_FIXED_PAGE_ALLOC 36

#define OPERATION_ALLOC_REGION 37
#define OPERATION_DEALLOC_REGION 38

// these operations are specific to OASYS
#define OPERATION_OASYS_DIFF_DO           75  
#define OPERATION_OASYS_DIFF_REDO         76  
#define OPERATION_OASYS_DIFF_UNDO         77  
#define OPERATION_OASYS_SEMIDIFF_DO       78
#define OPERATION_OASYS_SEMIDIFF_REDO     79

#define STORAGE_MANAGER_NAIVE_PAGE_ALLOC  1



/* number above should be less than number below */
#define MAX_OPERATIONS 80

//#define SLOT_TYPE_BASE PAGE_SIZE

/** This constant is used as a placeholder to mark slot locations that are invalid.
    @see slotted.c, indirect.c
*/
#define INVALID_SLOT  (-1)
/** This constant is used as a placeholder to mark slot locations that contain blobs. 
    @see slotted.c, indirect.c,  blobManager.c  */
#define BLOB_SLOT     (-2)
#define SLOT_TYPE_END (-3)

/** Initialized statically in transactional2.c */
extern const short SLOT_TYPE_LENGTHS[];
 
#define BLOB_THRESHOLD_SIZE (PAGE_SIZE-30)

#define BITS_PER_BYTE 8

/** Log entry types.  (Not to be confused with operation types, which are more interesting.) */


/*
  Definitions for different types of logs
*/
#define UPDATELOG 1
#define XBEGIN 2
#define XCOMMIT 3
#define XABORT 4
/* Folded into update log entries */
/*#define XALLOC 5*/
/** 
    XEND is used for after the pages touched by a transaction have
    been flushed to stable storage.
    
    @todo Actually write XEND entries to the log so that log
    truncation can be implemented!

*/
#define XEND 6
#define CLRLOG 7

/* Page types */

#define UNINITIALIZED_PAGE  0
#define SLOTTED_PAGE        1
#define INDIRECT_PAGE       2
#define LLADD_HEADER_PAGE   3
#define LLADD_FREE_PAGE     4
#define FIXED_PAGE          5
#define ARRAY_LIST_PAGE     6
#define BOUNDARY_TAG_PAGE   7

/* Record types */

#define UNINITIALIZED_RECORD 0
#define BLOB_RECORD          1
#define SLOTTED_RECORD       2
#define FIXED_RECORD         3

#endif
