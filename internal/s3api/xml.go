package s3api

import (
	"encoding/xml"
	"time"
)

const s3NS = "http://s3.amazonaws.com/doc/2006-03-01/"

// ---- ListBuckets ----

type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   Owner    `xml:"Owner"`
	Buckets BucketsW `xml:"Buckets"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type BucketsW struct {
	Bucket []BucketEntry `xml:"Bucket"`
}

type BucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// ---- CreateBucket ----

type CreateBucketConfiguration struct {
	XMLName            xml.Name `xml:"CreateBucketConfiguration"`
	LocationConstraint string   `xml:"LocationConstraint"`
}

// ---- ListObjectsV2 ----

type ListBucketResultV2 struct {
	XMLName               xml.Name        `xml:"ListBucketResult"`
	Xmlns                 string          `xml:"xmlns,attr"`
	Name                  string          `xml:"Name"`
	Prefix                string          `xml:"Prefix"`
	Delimiter             string          `xml:"Delimiter,omitempty"`
	MaxKeys               int             `xml:"MaxKeys"`
	KeyCount              int             `xml:"KeyCount"`
	IsTruncated           bool            `xml:"IsTruncated"`
	ContinuationToken     string          `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string          `xml:"NextContinuationToken,omitempty"`
	StartAfter            string          `xml:"StartAfter,omitempty"`
	Contents              []ObjectXML     `xml:"Contents"`
	CommonPrefixes        []CommonPrefix  `xml:"CommonPrefixes"`
	EncodingType          string          `xml:"EncodingType,omitempty"`
}

// ---- ListObjectsV1 ----

type ListBucketResultV1 struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Xmlns          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Marker         string         `xml:"Marker"`
	NextMarker     string         `xml:"NextMarker,omitempty"`
	MaxKeys        int            `xml:"MaxKeys"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []ObjectXML    `xml:"Contents"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes"`
}

type ObjectXML struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// ---- Multipart ----

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type CompleteMultipartUpload struct {
	XMLName xml.Name      `xml:"CompleteMultipartUpload"`
	Parts   []CompletePart `xml:"Part"`
}

type CompletePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type ListPartsResult struct {
	XMLName             xml.Name  `xml:"ListPartsResult"`
	Xmlns               string    `xml:"xmlns,attr"`
	Bucket              string    `xml:"Bucket"`
	Key                 string    `xml:"Key"`
	UploadID            string    `xml:"UploadId"`
	Initiator           Owner     `xml:"Initiator"`
	Owner               Owner     `xml:"Owner"`
	StorageClass        string    `xml:"StorageClass"`
	PartNumberMarker    int       `xml:"PartNumberMarker"`
	NextPartNumberMarker int      `xml:"NextPartNumberMarker"`
	MaxParts            int       `xml:"MaxParts"`
	IsTruncated         bool      `xml:"IsTruncated"`
	Parts               []PartXML `xml:"Part"`
}

type PartXML struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type ListMultipartUploadsResult struct {
	XMLName            xml.Name      `xml:"ListMultipartUploadsResult"`
	Xmlns              string        `xml:"xmlns,attr"`
	Bucket             string        `xml:"Bucket"`
	KeyMarker          string        `xml:"KeyMarker"`
	UploadIDMarker     string        `xml:"UploadIdMarker"`
	NextKeyMarker      string        `xml:"NextKeyMarker"`
	NextUploadIDMarker string        `xml:"NextUploadIdMarker"`
	Delimiter          string        `xml:"Delimiter,omitempty"`
	Prefix             string        `xml:"Prefix"`
	MaxUploads         int           `xml:"MaxUploads"`
	IsTruncated        bool          `xml:"IsTruncated"`
	Uploads            []UploadEntry `xml:"Upload"`
}

type UploadEntry struct {
	Key          string `xml:"Key"`
	UploadID     string `xml:"UploadId"`
	Initiator    Owner  `xml:"Initiator"`
	Owner        Owner  `xml:"Owner"`
	StorageClass string `xml:"StorageClass"`
	Initiated    string `xml:"Initiated"`
}

// ---- Delete (batch) ----

type DeleteRequest struct {
	XMLName xml.Name        `xml:"Delete"`
	Quiet   bool            `xml:"Quiet"`
	Objects []DeleteObjectK `xml:"Object"`
}

type DeleteObjectK struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
}

type DeleteResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Xmlns   string          `xml:"xmlns,attr"`
	Deleted []DeletedEntry  `xml:"Deleted"`
	Errors  []DeleteErrorE  `xml:"Error"`
}

type DeletedEntry struct {
	Key string `xml:"Key"`
}

type DeleteErrorE struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// ---- Tagging ----

type Tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	TagSet  TagSet   `xml:"TagSet"`
}

type TagSet struct {
	Tag []Tag `xml:"Tag"`
}

type Tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// ---- Copy ----

type CopyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

// ---- LocationConstraint / canned configs ----

type LocationConstraintResp struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr"`
	Value   string   `xml:",chardata"`
}

type AccessControlPolicy struct {
	XMLName           xml.Name             `xml:"AccessControlPolicy"`
	Owner             Owner                `xml:"Owner"`
	AccessControlList AccessControlList    `xml:"AccessControlList"`
}

type AccessControlList struct {
	Grant []Grant `xml:"Grant"`
}

type Grant struct {
	Grantee    Grantee `xml:"Grantee"`
	Permission string  `xml:"Permission"`
}

type Grantee struct {
	XMLName     xml.Name `xml:"Grantee"`
	Type        string   `xml:"xsi:type,attr"`
	XmlnsXSI    string   `xml:"xmlns:xsi,attr"`
	ID          string   `xml:"ID,omitempty"`
	DisplayName string   `xml:"DisplayName,omitempty"`
}

type VersioningConfig struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Xmlns   string   `xml:"xmlns,attr"`
	Status  string   `xml:"Status,omitempty"`
}

// fmtIso formats a time as ISO 8601 UTC suitable for S3 LastModified fields.
func fmtIso(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}
