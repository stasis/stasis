require 'stasis/raw'

module Stasis
  class StringIterator
    def StringIterator.value(xid, it)
      objptrptr = FFI::MemoryPointer.new :pointer
      len = Raw.Titerator_value(xid, it, objptrptr)
      if(len == -1) then return nil end

      objptr = objptrptr.get_pointer(0)
      # The slice is for bounds checking.
      str = objptr.null? ? nil : objptr.slice(0, len).get_string(0)
      # The iterator frees objptr during tupleDone.
      objptrptr.free
      return str
    end
    def StringIterator.key(xid, it)
      objptrptr = FFI::MemoryPointer.new :pointer
      len = Raw.Titerator_key(xid, it, objptrptr)
      if(len == -1) then return nil end

      objptr = objptrptr.get_pointer(0)
      # The slice is for bounds checking.
      str = objptr.null? ? nil : objptr.slice(0, len).get_string(0)
      # The iterator frees objptr during tupleDone.
      objptrptr.free
      return str
    end
  end
end
