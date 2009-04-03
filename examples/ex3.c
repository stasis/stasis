#include <stasis/transactional.h>
#include <stdio.h>
#include <assert.h>

typedef struct {
  recordid left;
  recordid right;
  int32_t  val;
} tree_node;

typedef struct {
  recordid node;
} tree_root;

typedef struct {
  pageid_t page;
  slotid_t slot;
  int val;
} tree_op_args;

static const int TREE_INSERT = OPERATION_USER_DEFINED(0);
static const int TREE_REMOVE = OPERATION_USER_DEFINED(1);

static inline int is_null(recordid r) {
  return (r.page == NULLRID.page &&
	  r.slot == NULLRID.slot &&
	  r.size == NULLRID.size);
}

static int insert_node(int xid, recordid rid, tree_node* node) {
  tree_node n;
  Tread(xid, rid, &n);
  if(node->val < n.val) {
    if(is_null(n.left)) {
      n.left = Talloc(xid, sizeof(tree_node));
      Tset(xid, rid, &n);
      Tset(xid, n.left, node);
      return 0;
    } else {
      return insert_node(xid,n.left,node);
    }
  } else if(node->val > n.val) {
    if(is_null(n.right)) {
      n.right = Talloc(xid, sizeof(tree_node));
      Tset(xid, rid, &n);
      Tset(xid, n.right, node);
      return 0;
    } else {
      return insert_node(xid,n.right,node);
    }
  } else {
    assert(node->val != n.val); // will always fail if we get here.
    abort();
  }
}

static int op_tree_insert(const LogEntry* e, Page* p) {
  // TODO: Latching
  assert(p == NULL);
  const tree_op_args* a = getUpdateArgs(e);

  recordid root_rid = { a->page, a->slot, sizeof(tree_root) };
  tree_root root;
  Tread(e->xid, root_rid, &root);

  tree_node node;
  node.val = a->val;
  node.left = NULLRID;
  node.right = NULLRID;

  if(is_null(root.node)) {
    root.node = Talloc(e->xid, sizeof(tree_node));
    Tset(e->xid, root_rid, &root);
    Tset(e->xid, root.node, &node);
    return 0;
  } else {
    return insert_node(e->xid, root_rid, &node);
  }
}

static recordid second_from_the_right(int xid, recordid rid) {
  tree_node n;
  Tread(xid, rid, &n);
  // return NULLRID if there is no node w/ non-null right branch.
  recordid last_rid = NULLRID;  
  while(!is_null(n.right)) {
    last_rid = rid;
    rid = n.right;
    Tread(xid, rid, &n);
  }
  // now, rid->right == null, last_rid points to rid.
  return last_rid;
}

static int remove_node(int xid, recordid rid, int val) {
  tree_node n;
  assert(!is_null(rid));
  Tread(xid, rid, &n);
  if(val < n.val) {
    if(remove_node(xid, n.left, val)) {
      n.left = NULLRID;
      Tset(xid, rid, &n);
    }
    return 0;
  } else if(val > n.val) {
    if(remove_node(xid, n.right, val)) {
      n.right = NULLRID;
      Tset(xid, rid, &n);
    }
    return 0;
  } else {
    if(is_null(n.left) && is_null(n.right)) {
      Tdealloc(xid, rid);
      return 1;
    } else if(is_null(n.left)) {
      tree_node right;
      Tread(xid, n.right, &right);
      Tset(xid, rid, &right);
      return 0;
    } else if(is_null(n.right)) {
      tree_node left;
      Tread(xid, n.left, &left);
      Tset(xid, rid, &left);
    } else {
      recordid internal_rid = second_from_the_right(xid, n.left);
      if(is_null(internal_rid)) {
	// rid.left.right is null
	tree_node left;
	Tread(xid, n.left, &left);
	assert(is_null(left.right));
	n.left = left.left;
	n.val = left.val;
	Tdealloc(xid, n.left);
      } else {
	// internal.right.right is null.
	tree_node internal; tree_node internal_right;

	Tread(xid, internal_rid,  &internal);
	Tread(xid, internal.right,&internal_right);

	Tdealloc(xid, internal.right);
	n.val = internal_right.val;
	internal.right = internal_right.left;
	assert(is_null(internal_right.right));

	Tset(xid, rid, &n);
	Tset(xid, internal_rid, &internal);
      }
    }
    return 0;
  }
}

static int op_tree_remove(const LogEntry* e, Page* p) {
  // TODO: Latching
  assert(p == NULL);
  const tree_op_args* a = getUpdateArgs(e);
  recordid root_rid = { a->page, a->slot, sizeof(tree_root) };
  return remove_node(e->xid, root_rid, a->val);
}

void TexampleTreeInsert(int xid, recordid tree, int val) {
  const tree_op_args arg = { tree.page, tree.slot, val };
  if(TnestedTopAction(xid, TREE_INSERT, (const byte*)&arg, sizeof(arg))) {
    abort();
  }
}

void TexampleTreeRemove(int xid, recordid tree, int val) {
  const tree_op_args arg = { tree.page, tree.slot, val };
  if(TnestedTopAction(xid, TREE_REMOVE, (const byte*)&arg, sizeof(arg))) {
    abort();
  }
}

int find_node(int xid, recordid node_rid, int val) {

  if(is_null(node_rid)) { return 0; }

  tree_node node;
  Tread(xid, node_rid, &node);

  if(val == node.val) {
    return 1;
  } else if(val < node.val) {
    return find_node(xid, node.left, val);
  } else {
    return find_node(xid, node.right, val);
  }
}
/**
   @return true iff the tree contains val
 */
int TexampleTreeContains(int xid, recordid root_rid, int val) {
  tree_root root;
  Tread(xid, root_rid, &root);
  return find_node(xid,root.node,val);
}

int main (int argc, char ** argv) {

  recordid rootEntry;
  {
    stasis_operation_impl op_ins = {
      TREE_INSERT,
      OPERATION_NOOP,
      TREE_REMOVE,
      op_tree_insert
    };
    stasis_operation_impl op_rem = {
      TREE_REMOVE,
      OPERATION_NOOP,
      TREE_INSERT,
      op_tree_remove
    };
    stasis_operation_table_init();
    stasis_operation_impl_register(op_ins);
    stasis_operation_impl_register(op_rem);
  }
  Tinit();

  if(TrecordType(INVALID_XID, ROOT_RECORD) == INVALID_SLOT) {
    tree_root root = {
      NULLRID
    };
    int xid = Tbegin();

    rootEntry = Talloc(xid, sizeof(root));
    assert(rootEntry.page == ROOT_RECORD.page &&
	   rootEntry.slot == ROOT_RECORD.slot);
    Tset(xid, rootEntry, &root);

    Tcommit(xid);

  } else {
    rootEntry.page = ROOT_RECORD.page;
    rootEntry.slot = ROOT_RECORD.slot;
    rootEntry.size = sizeof(tree_root);

    int xid = Tbegin();

    assert( ! TexampleTreeContains(xid, ROOT_RECORD, 1));
    assert(   TexampleTreeContains(xid, ROOT_RECORD, 2));
    assert( ! TexampleTreeContains(xid, ROOT_RECORD, 3));

    TexampleTreeRemove(xid, ROOT_RECORD, 2);

    Tcommit(xid);

  }

  assert(TrecordSize(INVALID_XID, ROOT_RECORD) == sizeof(tree_root));
  int xid1 = Tbegin();
  int xid2 = Tbegin();

  assert( ! TexampleTreeContains(xid1, ROOT_RECORD, 1));
  assert( ! TexampleTreeContains(xid1, ROOT_RECORD, 2));
  assert( ! TexampleTreeContains(xid1, ROOT_RECORD, 3));

  TexampleTreeInsert(xid1, ROOT_RECORD, 1);
  TexampleTreeInsert(xid2, ROOT_RECORD, 2);
  TexampleTreeInsert(xid1, ROOT_RECORD, 3);

  assert(   TexampleTreeContains(xid1, ROOT_RECORD, 1));
  assert(   TexampleTreeContains(xid1, ROOT_RECORD, 2));
  assert(   TexampleTreeContains(xid1, ROOT_RECORD, 3));

  Tabort(xid1);

  assert( ! TexampleTreeContains(xid1, ROOT_RECORD, 1));
  assert(   TexampleTreeContains(xid1, ROOT_RECORD, 2));
  assert( ! TexampleTreeContains(xid1, ROOT_RECORD, 3));

  Tcommit(xid2);
  Tdeinit();
}
