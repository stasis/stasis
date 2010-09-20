require 'stasis/raw'

module Stasis
  class Hash
    extend FFI::Library
    ffi_lib FFI::Library::LIBC
    attach_function 'free', [:pointer], :void  # called by Hash.lookup

    def Hash.alloc(xid)
      return Raw.ThashCreate xid, -1, -1
    end

    def Hash.insert(xid, rid, key, val)
      keylen = key.length+1
      vallen = val.length+1
      keyptr = FFI::MemoryPointer.new :char, keylen
      valptr = FFI::MemoryPointer.new :char, vallen
      keyptr.put_string(0, key)
      valptr.put_string(0, val)

      ret = Raw.ThashInsert xid, rid, keyptr, keylen, valptr, vallen;

      keyptr.free
      valptr.free
      return ret
    end
    
    def Hash.remove(xid, rid, key)
      keylen = key.length+1
      keyptr = FFI::MemoryPointer.new :char, keylen
      ret = Raw.ThashRemove xid, rid, keyptr, keylen
      keyptr.free
      return ret
    end

    def Hash.iterator(xid, rid)
      return Raw.ThashGenericIterator xid, rid
    end

    def Hash.lookup(xid, rid, key) 
      keylen = key.length+1
      keyptr = FFI::MemoryPointer.new :char, keylen
      keyptr.put_string(0, key)
      objptrptr = FFI::MemoryPointer.new :pointer
      len = Raw.ThashLookup(xid, rid, keyptr, keylen, objptrptr);
      objptr = objptrptr.get_pointer(0)
      str = objptr.null? ? nil : objptr.get_string(0)
      keyptr.free
      free objptr
      return str
    end
  end
end
