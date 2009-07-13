/*
 * segmentFile.h
 *
 *  Created on: Jul 9, 2009
 *      Author: sears
 */

#ifndef SEGMENTFILE_H_
#define SEGMENTFILE_H_

ssize_t Tpread(int xid, byte* buf, size_t count, off_t offset);
ssize_t Tpwrite(int xid, const byte * buf, size_t count, off_t offset);

stasis_operation_impl stasis_op_impl_segment_file_pwrite();
stasis_operation_impl stasis_op_impl_segment_file_pwrite_inverse();

#endif /* SEGMENTFILE_H_ */
