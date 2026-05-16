package index

type ObjectRecord struct {
	Schema       uint8             `json:"v"`
	Key          string            `json:"k"`
	Size         int64             `json:"sz"`
	ETag         string            `json:"etag"`
	ContentType  string            `json:"ct,omitempty"`
	ContentEnc   string            `json:"ce,omitempty"`
	ContentDisp  string            `json:"cd,omitempty"`
	CacheCtrl    string            `json:"cc,omitempty"`
	Expires      string            `json:"exp,omitempty"`
	CreatedAt    int64             `json:"cat"`
	ModifiedAt   int64             `json:"mat"`
	UserMeta     map[string]string `json:"um,omitempty"`
	Tags         map[string]string `json:"tg,omitempty"`
	StorageClass string            `json:"sc,omitempty"`
	Storage      StorageRef        `json:"st"`
}

type StorageRef struct {
	Mode   string `json:"m"` // "file" | "pack"
	Path   string `json:"p"` // relative to data_dir
	Offset int64  `json:"o"`
	Length int64  `json:"l"`
	PackID uint64 `json:"pid,omitempty"`
}

type BucketRecord struct {
	Schema     uint8  `json:"v"`
	Name       string `json:"n"`
	CreatedAt  int64  `json:"cat"`
	Versioning string `json:"ver,omitempty"`
}

type MultipartRecord struct {
	Schema    uint8        `json:"v"`
	UploadID  string       `json:"id"`
	Bucket    string       `json:"b"`
	Key       string       `json:"k"`
	Initiated int64        `json:"i"`
	Meta      ObjectRecord `json:"meta"`
	Parts     []PartRecord `json:"p"`
}

type PartRecord struct {
	Number     int    `json:"n"`
	ETag       string `json:"e"`
	Size       int64  `json:"sz"`
	Path       string `json:"p"`
	UploadedAt int64  `json:"u"`
}

type PackFileRecord struct {
	Schema    uint8  `json:"v"`
	PackID    uint64 `json:"id"`
	Path      string `json:"p"`
	Size      int64  `json:"sz"`
	Sealed    bool   `json:"sl"`
	LiveBytes int64  `json:"lb"`
	CreatedAt int64  `json:"c"`
}

const RecordSchema uint8 = 1
