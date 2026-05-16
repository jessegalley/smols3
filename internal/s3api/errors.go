package s3api

import (
	"encoding/xml"
	"net/http"
)

// S3 standard error codes.
type S3Error struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string   `xml:"Code"`
	Message    string   `xml:"Message"`
	Resource   string   `xml:"Resource,omitempty"`
	RequestID  string   `xml:"RequestId,omitempty"`
	BucketName string   `xml:"BucketName,omitempty"`
	Key        string   `xml:"Key,omitempty"`
	HTTPStatus int      `xml:"-"`
}

func (e *S3Error) Error() string { return e.Code + ": " + e.Message }

func newErr(code, msg string, status int) *S3Error {
	return &S3Error{Code: code, Message: msg, HTTPStatus: status}
}

var (
	ErrNoSuchBucket           = newErr("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
	ErrNoSuchKey              = newErr("NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
	ErrNoSuchUpload           = newErr("NoSuchUpload", "The specified multipart upload does not exist.", http.StatusNotFound)
	ErrBucketAlreadyOwnedByYou = newErr("BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", http.StatusConflict)
	ErrBucketAlreadyExists    = newErr("BucketAlreadyExists", "The requested bucket name is not available.", http.StatusConflict)
	ErrBucketNotEmpty         = newErr("BucketNotEmpty", "The bucket you tried to delete is not empty.", http.StatusConflict)
	ErrInvalidBucketName      = newErr("InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)
	ErrInvalidRequest         = newErr("InvalidRequest", "The request is malformed.", http.StatusBadRequest)
	ErrEntityTooLarge         = newErr("EntityTooLarge", "Your proposed upload exceeds the maximum allowed object size.", http.StatusBadRequest)
	ErrMissingContentLength   = newErr("MissingContentLength", "You must provide the Content-Length HTTP header.", http.StatusLengthRequired)
	ErrAccessDenied           = newErr("AccessDenied", "Access Denied.", http.StatusForbidden)
	ErrSignatureMismatch      = newErr("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", http.StatusForbidden)
	ErrNotImplemented         = newErr("NotImplemented", "A header you provided implies functionality that is not implemented.", http.StatusNotImplemented)
	ErrInternal               = newErr("InternalError", "An internal error occurred.", http.StatusInternalServerError)
	ErrInvalidPart            = newErr("InvalidPart", "One or more of the specified parts could not be found.", http.StatusBadRequest)
	ErrInvalidPartOrder       = newErr("InvalidPartOrder", "The list of parts was not in ascending order.", http.StatusBadRequest)
	ErrInvalidRange           = newErr("InvalidRange", "The requested range is not satisfiable.", http.StatusRequestedRangeNotSatisfiable)
	ErrMalformedXML           = newErr("MalformedXML", "The XML you provided was not well-formed or did not validate.", http.StatusBadRequest)
)

func writeS3Error(w http.ResponseWriter, r *http.Request, e *S3Error) {
	e2 := *e
	e2.Resource = r.URL.Path
	e2.RequestID = r.Header.Get("X-Amz-Request-Id")
	if e2.RequestID == "" {
		e2.RequestID = "smols3-req"
	}
	body, _ := xml.MarshalIndent(&e2, "", "  ")
	w.Header().Set("Content-Type", "application/xml")
	status := e.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}
