require 'stasis/hash'
require 'test/unit'
module Stasis
  class TestHash < Test::Unit::TestCase

    def setup
      `rm -f storefile.txt logfile.txt`
    end
    
    def teardown
      `rm -f storefile.txt logfile.txt`
    end
    
    def test_combined
      assert_equal(0, Raw.Tinit)
      xid = Raw.Tbegin
      rid = Hash.alloc(xid)
      assert_equal(0,     Hash.insert(xid, rid, "foo", "bar"))
      assert_equal("bar", Hash.lookup(xid, rid, "foo"))
                          Raw.Tcommit xid
                          xid = Raw.Tbegin
      assert_equal(1,     Hash.insert(xid, rid, "foo", "baz"))
      assert_equal(0,     Hash.insert(xid, rid, "bar", "bat"))
      assert_equal("baz", Hash.lookup(xid, rid, "foo"))
      assert_equal("bat", Hash.lookup(xid, rid, "bar"))
                          Raw.Tabort xid
      assert_equal("bar", Hash.lookup( -1, rid, "foo"))
      assert_equal(nil,   Hash.lookup( -1, rid, "bar"))
      assert_equal(0,     Raw.Tdeinit)
      assert_equal(0,     Raw.Tinit)
      assert_equal("bar", Hash.lookup( -1, rid, "foo"))
      assert_equal(nil,   Hash.lookup( -1, rid, "bar"))
      xid = Raw.Tbegin
      assert_equal(0,     Hash.insert(xid, rid, "bar", "bat"))
      assert_equal("bat", Hash.lookup(xid,rid,"bar"))
      assert_equal(0,     Raw.Tdeinit)
    end
  end
end
