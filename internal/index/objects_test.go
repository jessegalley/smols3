package index

import (
	"path/filepath"
	"testing"
)

func newTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreateBucket("b"); err != nil {
		t.Fatal(err)
	}
	return db
}

func mustPut(t *testing.T, db *DB, bucket string, keys ...string) {
	t.Helper()
	for _, k := range keys {
		rec := ObjectRecord{Schema: RecordSchema, Key: k, Size: 1, ETag: "x"}
		if err := db.PutObject(bucket, rec); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
}

func keys(objs []ObjectRecord) []string {
	out := make([]string, len(objs))
	for i, o := range objs {
		out[i] = o.Key
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestListPlain(t *testing.T) {
	db := newTestDB(t)
	mustPut(t, db, "b", "a", "b", "c")
	res, err := db.ListObjects("b", ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := keys(res.Objects); !eq(got, []string{"a", "b", "c"}) {
		t.Fatalf("plain list: got %v", got)
	}
	if res.IsTruncated {
		t.Fatal("plain list should not be truncated")
	}
}

func TestListPrefix(t *testing.T) {
	db := newTestDB(t)
	mustPut(t, db, "b", "alpha", "beta", "bravo", "charlie")
	res, _ := db.ListObjects("b", ListOptions{Prefix: "b"})
	if got := keys(res.Objects); !eq(got, []string{"beta", "bravo"}) {
		t.Fatalf("prefix list: got %v", got)
	}
}

func TestListDelimiter(t *testing.T) {
	db := newTestDB(t)
	mustPut(t, db, "b", "dir1/a", "dir1/b", "dir2/c", "top")
	res, _ := db.ListObjects("b", ListOptions{Delimiter: "/"})
	if got := keys(res.Objects); !eq(got, []string{"top"}) {
		t.Fatalf("delim objects: got %v", got)
	}
	if !eq(res.CommonPrefixes, []string{"dir1/", "dir2/"}) {
		t.Fatalf("common prefixes: got %v", res.CommonPrefixes)
	}
}

func TestListPagination(t *testing.T) {
	db := newTestDB(t)
	all := []string{"k01", "k02", "k03", "k04", "k05"}
	mustPut(t, db, "b", all...)

	res, _ := db.ListObjects("b", ListOptions{MaxKeys: 2})
	if !res.IsTruncated {
		t.Fatal("expected truncation on page1")
	}
	if got := keys(res.Objects); !eq(got, []string{"k01", "k02"}) {
		t.Fatalf("page1: %v", got)
	}
	res2, _ := db.ListObjects("b", ListOptions{MaxKeys: 2, Token: res.NextToken})
	if got := keys(res2.Objects); !eq(got, []string{"k03", "k04"}) {
		t.Fatalf("page2: %v", got)
	}
	if !res2.IsTruncated {
		t.Fatal("expected truncation on page2")
	}
	res3, _ := db.ListObjects("b", ListOptions{MaxKeys: 2, Token: res2.NextToken})
	if got := keys(res3.Objects); !eq(got, []string{"k05"}) {
		t.Fatalf("page3: %v", got)
	}
	if res3.IsTruncated {
		t.Fatal("page3 should not be truncated")
	}
}

func TestListDelimiterPaginationAcrossPrefixes(t *testing.T) {
	db := newTestDB(t)
	mustPut(t, db,
		"b",
		"a/1", "a/2", "a/3",
		"b/1", "b/2",
		"c/1",
		"z",
	)
	// MaxKeys=2, delimiter=/. Page1 should emit a/, b/ (CommonPrefixes), truncate.
	res, _ := db.ListObjects("b", ListOptions{Delimiter: "/", MaxKeys: 2})
	if !res.IsTruncated {
		t.Fatal("expected page1 truncation")
	}
	if !eq(res.CommonPrefixes, []string{"a/", "b/"}) {
		t.Fatalf("page1 cps: %v", res.CommonPrefixes)
	}
	// Page2 should resume at c/, z.
	res2, _ := db.ListObjects("b", ListOptions{Delimiter: "/", MaxKeys: 5, Token: res.NextToken})
	if !eq(res2.CommonPrefixes, []string{"c/"}) {
		t.Fatalf("page2 cps: %v", res2.CommonPrefixes)
	}
	if got := keys(res2.Objects); !eq(got, []string{"z"}) {
		t.Fatalf("page2 objs: %v", got)
	}
	if res2.IsTruncated {
		t.Fatal("page2 should not be truncated")
	}
}

func TestListStartAfter(t *testing.T) {
	db := newTestDB(t)
	mustPut(t, db, "b", "a", "b", "c", "d")
	res, _ := db.ListObjects("b", ListOptions{StartAfter: "b"})
	if got := keys(res.Objects); !eq(got, []string{"c", "d"}) {
		t.Fatalf("start-after: %v", got)
	}
}

func TestListEmptyBucket(t *testing.T) {
	db := newTestDB(t)
	res, err := db.ListObjects("b", ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Objects) != 0 || len(res.CommonPrefixes) != 0 || res.IsTruncated {
		t.Fatalf("empty bucket should be empty: %+v", res)
	}
}

func TestListNonexistentBucket(t *testing.T) {
	db := newTestDB(t)
	_, err := db.ListObjects("nope", ListOptions{})
	if err == nil {
		t.Fatal("expected error on missing bucket")
	}
}
