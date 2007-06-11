
lladdIterator_t * arrayIterator(byte * keyArray, unsigned int keySize,  
				byte * valueArray, unsigned int valueSize, 
				unsigned int elementCount);
void arrayIterator_close(int xid, void * impl);
int  arrayIterator_next (int xid, void * impl);
int  arrayIterator_key  (int xid, void * impl, byte ** key);
int  arrayIterator_value(int xid, void * impl, byte ** value);
