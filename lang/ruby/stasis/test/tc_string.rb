require 'stasis/string'
require 'test/unit'

class TestStrings < Test::Unit::TestCase

  def setup
    `rm -f storefile.txt logfile.txt`
  end

  def teardown
    `rm -f storefile.txt logfile.txt`
  end

  def test_abort
    assert_equal(0, Stasis::Raw.Tinit)
    xid = Stasis::Raw.Tbegin
    rid = Stasis::String.put_new xid, "Hello world"
    str = Stasis::String.get xid, rid
    assert_equal("Hello world", str)
    Stasis::Raw.Tcommit xid;
    xid = Stasis::Raw.Tbegin
    assert(Stasis::String.set xid, rid, "G'bye world")
    str = Stasis::String.get xid, rid
    assert_equal("G'bye world", str)
    Stasis::Raw.Tabort xid;
    str = Stasis::String.get(-1, rid)
    assert_equal("Hello world", str)
    assert_equal(0, Stasis::Raw.Tdeinit)
  end


  def test_recover
    assert_equal(0, Stasis::Raw.Tinit)
    xid = Stasis::Raw.Tbegin
    rid = Stasis::String.put_new xid, "Hello world"
    str = Stasis::String.get xid, rid
    assert_equal("Hello world", str)
    Stasis::Raw.Tcommit xid;
    xid = Stasis::Raw.Tbegin
    assert(Stasis::String.set xid, rid, "G'bye world")
    str = Stasis::String.get xid, rid
    assert_equal("G'bye world", str)
    assert_equal(0, Stasis::Raw.Tdeinit)
    assert_equal(0, Stasis::Raw.Tinit)
    str = Stasis::String.get(-1, rid)
    assert_equal("Hello world", str)
    assert_equal(0, Stasis::Raw.Tdeinit)
  end

  ### XXX check for memory leaks...
end

