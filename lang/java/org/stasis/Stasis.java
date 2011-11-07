package org.stasis;

public class Stasis {
	// Stasis operations
	public static native int			init();
	public static native void			deinit();

	public static native long[]      root_record();
	
	public static boolean recordid_equals(long[] a, long[] b) { return a[0]==b[0]&&a[1]==b[1]; }
	
	// Transaction initiation
	public static native long			begin();
	public static native void			commit(long xid);
	public static native void			abort(long xid);
	public static native void			prepare(long xid);
	
	// Record operations
	public static native long			record_type(long xid, long[] rid);

	// LinearHashNTA
	public static native long[]		hash_create(long xid);
	public static native void			hash_delete(long xid, long[] rid); // TODO need way to drop tables from lincoln...
	public static native long			hash_cardinality(long xid, long[] rid);
	public static native byte[]		hash_insert(long xid, long[] rid, byte[] keybytes, byte[] valbytes);
	public static native byte[]		hash_remove(long xid, long[] rid, byte[] keybytes);
	public static native byte[]		hash_lookup(long xid, long[] rid, byte[] keybytes);
	public static native byte[]		hash_iterator(long xid, long[] rid);

	// Generic iterator interface 
	public static native void			iterator_close(long xid, byte[] it);
	public static native boolean	iterator_next(long xid, byte[] it);
	public static native byte[]		iterator_key(long xid, byte[] it);
	public static native byte[]		iterator_value(long xid, byte[] it);
	public static native void			iterator_tuple_done(long xid, byte[] it);

	public static void loadLibrary() {
		System.loadLibrary("stasisjni");
	}
	
	public static void main(String[] arg) {
		loadLibrary();
		System.out.println("Tinit()");
		init();
		System.out.println("Stasis is running");
		System.out.println("Tbegin()");
		long xid = begin();
		long[] root = root_record();
		long rootType =  record_type(xid,root);
		System.out.println("Root record (page: " + root[0] + ", slot: " + root[1] + ")  type is " + rootType);
		if(rootType == -1) {
			System.out.println("Uninitialized store");
			System.out.println("Creating root record");
			long[] hashRec = hash_create(xid);
			if(!recordid_equals(hashRec,root)) {
				throw new IllegalStateException("Bootstrapping didn't set the root record!!");
			}
		}

		System.out.println("Tcommit()");
		commit(xid);
		deinit();
		System.out.println("Successfully shut down.  Exiting.");
	}
}
