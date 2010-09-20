require 'stasis/string'
require 'stasis/hash'
require 'stasis/string_iterator'

class Stasis
  Stasis.init_count = 0;
  def initialize(xid=nil, rid=nil)
    if Stasis.init_count == 0 then
      Raw.Tinit
    end
    Stasis.init_count++;
    if rid == nil then
      rid = Raw.ROOT_RID
    end
    if xid == nil then
      @xid = -1
      @autoxact = true
    else
      @xid = xid
      @autoxact = false
    end
  end
