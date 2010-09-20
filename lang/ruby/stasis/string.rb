require 'stasis/raw'

module Stasis
  class String

    def String.alloc(xid, str)
      ## str.length is apparently the size of the underlying buffer.
      rid = Raw.Talloc(xid, str.length+1) 
      return rid
    end
    
    def String.put_new(xid, str)
      rid = String.alloc(xid, str)
      if(String.set(xid, rid, str)) 
        return rid
      else
        return nil # This will surely crash the interpreter.  Oh well.
      end
    end

    def String.get(xid, rid)
      objptr = FFI::MemoryPointer.new :char, Raw.TrecordSize(xid, rid);
      Raw.Tread(xid, rid, objptr);
      str = objptr.get_string(0)
      objptr.free
      return str
    end
    
    def String.set(xid, rid, str)
      if(str.length + 1 > Raw.TrecordSize(xid, rid))
        return false # throw exception?
      else
        objptr = FFI::MemoryPointer.new :char, str.length+1
        objptr.put_string(0, str);
        Raw.Tset(xid, rid, objptr)
        objptr.free
        return true
      end
    end
  end
end
