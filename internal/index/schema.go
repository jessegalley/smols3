package index

// SchemaVersion is the current on-disk index schema version. Bump when changing
// the keyspace or any persistent record shape in a non-additive way.
const SchemaVersion uint32 = 1

// Top-level bbolt bucket names.
var (
	bktMeta        = []byte("meta")
	bktBuckets     = []byte("buckets")
	bktObjects     = []byte("obj")    // sub-buckets keyed by S3-bucket name
	bktMultipart   = []byte("mp")     // sub-buckets keyed by S3-bucket name
	bktPacks       = []byte("packs")  // sub-buckets keyed by S3-bucket name
	bktPacksActive = []byte("packs_active")
)

// Meta keys.
var (
	metaSchemaVersion = []byte("schema_version")
	metaServerID      = []byte("server_id")
)
