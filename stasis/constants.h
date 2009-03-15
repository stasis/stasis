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
 * @todo Sometime, LLADD's \#includes need to be cleaned up.  In
 * particular, we should make sure everything directly or indirectly
 * includes this file, common.h, and constants.h 
 *
 * @ingroup LLADD_CORE
 *
 * $Id$
 */

#ifndef __CONSTANTS_H__
#define __CONSTANTS_H__

/*#define DEBUG 1*/

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

#ifndef MAX_BUFFER_SIZE
//#define MAX_BUFFER_SIZE 100003 
#define MAX_BUFFER_SIZE 20029
//#define MAX_BUFFER_SIZE 10007 
//#define MAX_BUFFER_SIZE 5003
//#define MAX_BUFFER_SIZE 2003 
//#define MAX_BUFFER_SIZE 4006 
/* #define MAX_BUFFER_SIZE 71  */
/*#define MAX_BUFFER_SIZE 7 */
#endif

#define BUFFER_MANAGER_REOPEN 0 
#define BUFFER_MANAGER_HASH   1
#define BUFFER_MANAGER_MEM_ARRAY 2
#define BUFFER_MANAGER_DEPRECATED_HASH 3

#define BUFFER_MANAGER_FILE_HANDLE_NON_BLOCKING 0
#define BUFFER_MANAGER_FILE_HANDLE_FILE 1
#define BUFFER_MANAGER_FILE_HANDLE_PFILE 2
#define BUFFER_MANAGER_FILE_HANDLE_DEPRECATED 3

#define IO_HANDLE_FILE 1
#define IO_HANDLE_PFILE 2

#define LOG_TO_FILE   0
#define LOG_TO_MEMORY 1

#define VOLATILE   0
#define PERSISTENT 1
#define DURABLE    2

#define MAX_TRANSACTIONS 1000

/** Operation types */

#define OPERATION_SET          0
#define OPERATION_SET_INVERSE  1
#define OPERATION_INCREMENT    2
#define OPERATION_DECREMENT    3
#define OPERATION_ALLOC        4
#define OPERATION_PREPARE      5
#define OPERATION_SET_LSN_FREE 6
#define OPERATION_SET_LSN_FREE_INVERSE 7
#define OPERATION_DEALLOC      8
#define OPERATION_REALLOC      9

#define OPERATION_PAGE_SET_RANGE     10
#define OPERATION_PAGE_SET_RANGE_INVERSE 11
// 12
// 13
//#define OPERATION_UPDATE_FREELIST 14
//#define OPERATION_UPDATE_FREELIST_INVERSE 15
//#define OPERATION_FREE_PAGE   16
//#define OPERATION_ALLOC_FREED 17
//#define OPERATION_UNALLOC_FREED 18

#define OPERATION_NOOP        19

#define OPERATION_ARRAY_LIST_ALLOC 21
#define OPERATION_INITIALIZE_PAGE 22

#define OPERATION_SET_RANGE 27
#define OPERATION_SET_RANGE_INVERSE 28
#define OPERATION_LINKED_LIST_INSERT 29
#define OPERATION_LINKED_LIST_REMOVE 30
#define OPERATION_LINEAR_HASH_INSERT 31
#define OPERATION_LINEAR_HASH_REMOVE 32

//#define OPERATION_SET_RAW    33
//#define OPERATION_INSTANT_SET_RAW 34

#define OPERATION_ALLOC_BOUNDARY_TAG 35

#define OPERATION_FIXED_PAGE_ALLOC 36

#define OPERATION_ALLOC_REGION 37
#define OPERATION_ALLOC_REGION_INVERSE 38

#define OPERATION_DEALLOC_REGION 39
#define OPERATION_DEALLOC_REGION_INVERSE 40

// these operations are specific to OASYS
#define OPERATION_OASYS_DIFF_DO           75  
#define OPERATION_OASYS_DIFF_REDO         76  
#define OPERATION_OASYS_DIFF_UNDO         77  
#define OPERATION_OASYS_SEMIDIFF_DO       78
#define OPERATION_OASYS_SEMIDIFF_REDO     79

/* Storage managers */
#define STORAGE_MANAGER_TALLOC 1
#define STORAGE_MANAGER_NAIVE_PAGE_ALLOC  2

/* number above should be less than number below */
#define MAX_OPERATIONS 80

//#define SLOT_TYPE_BASE PAGE_SIZE

#define INVALID_XID (-1)

/** This constant is used as a placeholder to mark slot locations that are invalid.
    @see slotted.c, indirect.c
*/
#define INVALID_SLOT  (-1)
/** This constant is used as a placeholder to mark slot locations that contain blobs. 
    @see slotted.c, indirect.c,  blobManager.c  */
#define BLOB_SLOT     (-2)
#define NORMAL_SLOT  (-3)
#define SLOT_TYPE_END (-4)

#define INVALID_PAGE (-1)

/** Initialized statically in transactional2.c */
extern const short SLOT_TYPE_LENGTHS[];
 
#define BLOB_THRESHOLD_SIZE (PAGE_SIZE-30)

#define BITS_PER_BYTE 8

/** Log entry types.  (Not to be confused with operation types, which are more interesting.) */


/*
  Definitions for different types of logs
*/
/* A log type reservered for the log implementation.  Its format
   depends on the physical log's implementation.  Most other code
   (recovery, abort) should be prepared to accept and ignore these
   entries. */
#define INTERNALLOG 0
#define XBEGIN 1
#define XCOMMIT 2
#define XABORT 3
#define UPDATELOG 4
/** 
    XEND is used for after the pages touched by a transaction have
    been flushed to stable storage.

    @todo Actually write XEND entries to the log so that we can
    use analysis to optimize redo.
*/
#define XEND 6
#define CLRLOG 7

#define XPREPARE 8

/* Page types */

#define UNINITIALIZED_PAGE  0
#define SLOTTED_PAGE        1
#define INDIRECT_PAGE       2
#define LLADD_HEADER_PAGE   3
#define LLADD_FREE_PAGE     4
#define FIXED_PAGE          5
#define ARRAY_LIST_PAGE     6
#define BOUNDARY_TAG_PAGE   7
#define BLOB_PAGE           8
#define LSM_ROOT_PAGE       9
#define SLOTTED_LSN_FREE_PAGE 10
#define USER_DEFINED_PAGE(n) (100+n)  // 0 <= n < 155
#define MAX_PAGE_TYPE       255

#define REGION_FIRST_TAG   1
#define TALLOC_REGION_SIZE 100  // Pages

#define FILE_PERM (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)
//#define LOG_MODE  (O_CREAT | O_RDWR | O_SYNC)

#define MAX_LSM_COMPARATORS 16
#define MAX_LSM_PAGE_INITIALIZERS 256
#define MAX_LSM_PAGE_ALLOCATORS 16

#define MAX_ITERATOR_TYPES 20
#define LINEAR_HASH_NTA_ITERATOR 0
#define ARRAY_ITERATOR           1
#define LOG_MEMORY_ITERATOR      2
#define POINTER_ITERATOR         3

#define USER_DEFINED_ITERATOR 10



#endif
