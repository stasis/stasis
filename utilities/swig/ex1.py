#!/usr/bin/python2.4
#

"""Example 1 in Python
"""

import stasis
import sys
import array

def runit(argv):
  print "init"
  stasis.Tinit()

  # First transaction, for writing
  print "begin & alloc"
  xid = stasis.Tbegin();
  record_id = stasis.Talloc(xid, 4);

  print "update"
  write_data = array.array('i', (42,))
  data_out = write_data.tostring()
  assert len(data_out) == 4

  stasis.TupdateStr(xid, record_id, data_out, stasis.OPERATION_SET)
  stasis.Tcommit(xid)

  # Second transaction, for reading
  print "read"
  xid = stasis.Tbegin()

  # Read from Stasis into memory for data_in
  data_in = '1234'
  stasis.TreadStr(xid, record_id, data_in)
  read_data = array.array('i', data_in)
  assert read_data[0] == 42

  print "done"
  stasis.Tdealloc(xid, record_id)
  stasis.Tabort(xid)

  stasis.Tdeinit()


if __name__ == "__main__":
  runit(sys.argv)
