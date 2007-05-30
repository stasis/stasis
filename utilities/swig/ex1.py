#!/usr/bin/python2.4
#

"""Example 1 in Python
"""

import lladd
import sys
import array

def runit(argv):
  print "init"
  lladd.Tinit()

  # First transaction, for writing
  print "begin & alloc"
  xid = lladd.Tbegin();
  record_id = lladd.Talloc(xid, 4);

  print "update"
  write_data = array.array('l', (42,))
  data_out = write_data.tostring()
  assert len(data_out) == 4

  lladd.TupdateStr(xid, record_id, data_out, lladd.OPERATION_SET)
  lladd.Tcommit(xid)

  # Second transaction, for reading
  print "read"
  xid = lladd.Tbegin()

  # Read from Stasis into memory for data_in
  data_in = '1234'
  lladd.TreadStr(xid, record_id, data_in)
  read_data = array.array('l', data_in)
  assert read_data[0] == 42

  print "done"
  lladd.Tdealloc(xid, record_id)
  lladd.Tabort(xid)

  lladd.Tdeinit()


if __name__ == "__main__":
  runit(sys.argv)
