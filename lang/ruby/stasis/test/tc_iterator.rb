require 'stasis/hash'
require 'stasis/string_iterator'
require 'test/unit'

module Stasis
  class TestIterator < Test::Unit::TestCase
    def setup
      `rm -f storefile.txt logfile.txt`
    end
    def teardown
      `rm -f storefile.txt logfile.txt`
    end

    def test_hash_iterator
      assert_equal(0, Raw.Tinit)
      xid = Raw.Tbegin
      rid = Hash.alloc(xid)
      h = {}
      (0..100).each {
        |x|
        assert_equal(0, Hash.insert(xid, rid, x.to_s, (x*10).to_s))
        h[x.to_s] = (x*10).to_s
      }

      i = {}
      it = Hash.iterator(xid, rid)

      while(0 != Raw.Titerator_next(xid, it)) 
        i[StringIterator.key(xid, it)] = StringIterator.value(xid, it)
      end

      Raw.Titerator_close(xid, it)
      assert_equal(h, i)
      
      Raw.Tcommit xid
      Raw.Tdeinit
    end
  end
end
