package stasis.jni;

import java.util.Arrays;
import java.util.Iterator;

import jol.core.Runtime;
import jol.types.basic.Tuple;
import jol.types.basic.TupleSet;
import jol.types.exception.BadKeyException;
import jol.types.exception.UpdateException;
import jol.types.table.Key;
import jol.types.table.StasisTable;
import jol.types.table.TableName;

public class LinearHashNTA extends StasisTable {
	static LinearHashNTA catalog = null;

    protected final Tuple header;
    private final long[] rid;

    protected Tuple registerTable(TableName name, Key key, Class[] type) throws UpdateException {
		Tuple header = new Tuple(rid[0], rid[1], key, attributeTypes);
		Tuple nameTup = new Tuple(name);

		Tuple row = catalog.key().reconstruct(nameTup, header);
		catalog.insert(row);

		return row;

    }
    protected LinearHashNTA(Runtime context) throws UpdateException {
		super(context, CATALOG_NAME, CATALOG_KEY, CATALOG_COLTYPES);
		key = new Key(0);
		long[] rootRid = Stasis.root_record();
		synchronized (xactTable) {
			long type = Stasis.record_type(ts.xid, rootRid);
			if(type == -1) {
				ts.dirty = true;
				rid = Stasis.hash_create(ts.xid);
				if(rid[0] != rootRid[0] || rid[1] != rootRid[1]) {
					throw new IllegalStateException();
				}
			} else {
				rid = new long[3];
				rid[0] = rootRid[0];
				rid[1] = rootRid[1];
			}
			header = CATALOG_SCHEMA;
		}
	}
	public LinearHashNTA(Runtime context, TableName name, Key key,
			Class[] attributeTypes) throws UpdateException {
		super(context, name, key, attributeTypes);
		int port = context.getPort();
		if(catalog == null) {
			catalog = new LinearHashNTA(context);  // open catalog based on recordid
			catalog.registerTable(CATALOG_NAME, CATALOG_KEY, CATALOG_COLTYPES);
		}
		TableName n = new TableName(port + ":" + name.scope, name.name);
		TupleSet headerSet;
		try {
			headerSet = catalog.primary().lookupByKey(n);
		} catch (BadKeyException e) {
			throw new IllegalStateException(e);
		}

		Tuple catalogEntry;
		synchronized (xactTable) {
			if(headerSet.isEmpty()) {
				ts.dirty = true;
				rid = Stasis.hash_create(ts.xid);
//				System.out.println("Alloced stasis table: " + n + ", " + key);
				catalogEntry = registerTable(n, key, attributeTypes);
			} else {
				catalogEntry = headerSet.iterator().next();
				rid = new long[3];
			}
		}
		header = catalog.primary().key().projectValue(catalogEntry);
		rid[0] = (Long)header.value(0);
		rid[1] = (Long)header.value(1);
	}

	@Override
	protected boolean add(byte[] keybytes, byte[] valbytes)
			throws UpdateException {
		synchronized (xactTable) {
			ts.dirty = true;
			byte[] oldvalbytes = Stasis.hash_insert(ts.xid, rid, keybytes, valbytes);
			if(oldvalbytes != null) {
				return (!Arrays.equals(valbytes, oldvalbytes));
			}
			return oldvalbytes == null;
		}
	}

	@Override
	public Long cardinality() {
		return Stasis.hash_cardinality(-1, rid);
	}

	@Override
	protected boolean remove(byte[] keybytes, byte[] valbytes)
			throws UpdateException {
		synchronized (xactTable) {
			// 'bug for bug' compatible w/ BasicTable
			byte[] oldvalbytes = lookup(keybytes);
			if(oldvalbytes == null || ! Arrays.equals(oldvalbytes, valbytes)) {
				return false;
			} else {
				Stasis.hash_remove(ts.xid, rid, keybytes);
				ts.dirty = true;
				return true;
			}
			// preferred implementation follows.
			/*byte[] oldvalbytes = Stasis.hash_remove(ts.xid, rid, keybytes);
			if(oldvalbytes != null && ! Arrays.equals(valbytes, oldvalbytes)) {
				throw new UpdateException("attempt to remove non-existant tuple");
			}
			ts.dirty = true;

			return oldvalbytes != null; */
		}
	}

	@Override
	protected byte[] lookup(byte[] keybytes) {
	    if(keybytes == null) throw new NullPointerException("keybytes is null!");
	    if(rid == null) throw new NullPointerException("rid is null!");
		return Stasis.hash_lookup(-1, rid, keybytes);
	}

	@Override
	protected Iterator<byte[][]> tupleBytes() {
		return new Iterator<byte[][]>() {
			private byte[] it = Stasis.hash_iterator(-1, rid);

			private byte[][] current = new byte[2][];
			private byte[][] next = new byte[2][];

			private boolean hadNext = true;
			Iterator<byte[][]> init() {
				hadNext = Stasis.iterator_next(-1, it);
				if(hadNext) {
					next[0] = Stasis.iterator_key(-1,it);
					next[1] = Stasis.iterator_value(-1, it);
					Stasis.iterator_tuple_done(-1, it);
				} else {
					Stasis.iterator_close(-1, it);
				}
				return this;
			}

			public boolean hasNext() {
				return hadNext;
			}

			public byte[][] next() {
				if(hadNext) {
					current = next;
					next = new byte[2][];

					hadNext = Stasis.iterator_next(-1,it);
					if(hadNext) {
						next[0] = Stasis.iterator_key(-1,it);
						next[1] = Stasis.iterator_value(-1,it);
						Stasis.iterator_tuple_done(-1,it);
					} else {
						Stasis.iterator_close(-1, it);
					}
					return current;
				} else {
					throw new IllegalStateException("next() called after end of iterator");
				}
			}

			public void remove() {
				throw new UnsupportedOperationException("No support for removal via table iterators yet...");
			}

			@Override
			protected void finalize() throws Throwable {
				try {
					if(hadNext)
						throw new IllegalStateException("detected non-exhausted iterator in finalize()");
				} finally {
					super.finalize();
				}
			}
		}.init();
	}

}
