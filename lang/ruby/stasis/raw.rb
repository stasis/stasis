require 'ffi'

module Stasis
  class Raw
    extend FFI::Library

    ## Stasis typedefs

    typedef :uchar, :byte
    typedef :int, :xid
    typedef :int64, :lsn
    typedef :int64, :pageid
    typedef :int32, :slotid
    typedef :int16, :pageoff
    typedef :int16, :pagetype
    typedef :pointer, :iterator
    typedef :pointer, :bytes

    # Not a managed struct, won't be GC'ed
    class RecordId < FFI::Struct
      layout :page, :pageid,
             :slot, :slotid,
             :size, :int64,
    end

    Stasis::ROOT_RID = RecordId.new
    Stasis::ROOT_RID[:page] = 1
    Stasis::ROOT_RID[:slot] = 0
    Stasis::ROOT_RID[:size] = -1
    
    Stasis::NULLRID = RecordId.new
    Stasis::NULLRID[:page] = 0
    Stasis::NULLRID[:slot] = 0
    Stasis::NULLRID[:size] = -1

    typedef RecordId.by_value, :recordid

    ## Functions exported by libstasis.so

    ffi_lib "stasis"
    attach_function 'Tinit', [ ], :int
    attach_function 'Tdeinit', [ ], :int
    attach_function 'Tbegin', [ ], :xid
    attach_function 'Tcommit', [ :xid ], :int
    attach_function 'TsoftCommit', [ :xid ], :int
    attach_function 'TforceCommits', [ ], :void
    attach_function 'Tabort', [ :xid ], :int
    attach_function 'Tprepare', [ :xid ], :int
    attach_function 'Talloc', [ :xid, :int ], :recordid
    attach_function 'TrecordType', [ :xid, :recordid ], :int
    attach_function 'TrecordSize', [ :xid, :recordid ], :int
    attach_function 'Tset', [ :xid, :recordid, :bytes ], :int
    # the string is an out parameter
    attach_function 'Tread', [ :xid, :recordid, :bytes ], :int

    # The second two parameters should be -1.
    attach_function 'ThashCreate', [ :xid, :int, :int ], :recordid
    attach_function 'ThashInsert', [ :xid, :recordid,
                                     :bytes, :int, 
                                     :bytes, :int ], :int
    attach_function 'ThashRemove', [ :xid, :recordid,
                                     :bytes, :int ], :int
    ## Note: The pointer is an OUT param
    attach_function 'ThashLookup', [ :xid, :recordid,
                                     :bytes, :int, :pointer ], :int
  
    attach_function 'ThashGenericIterator', [ :xid, :recordid ], :iterator
  
    attach_function 'Titerator_next', [:xid, :iterator], :int
    ## Note: The pointer is an OUT param
    attach_function 'Titerator_key', [:xid, :iterator, :pointer], :int
    ## Note: The pointer is an OUT param
    attach_function 'Titerator_value', [:xid, :iterator, :pointer], :int
    attach_function 'Titerator_tupleDone', [:xid, :iterator ], :void
    attach_function 'Titerator_close', [:xid, :iterator], :void
  end
end
