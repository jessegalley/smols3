// Package sigv4 verifies AWS Signature Version 4 on incoming requests.
// It supports both header-based (Authorization: AWS4-HMAC-SHA256 ...) and
// query-param-based (presigned URL) signing.
//
// Body hash verification is intentionally relaxed: the x-amz-content-sha256
// header value (or "UNSIGNED-PAYLOAD") is included in the canonical request
// as the claimed payload hash without re-hashing the body. This is sufficient
// for a test server — it catches malformed signing while sidestepping the
// streaming chunked-payload signature format used by aws-cli.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	algorithm = "AWS4-HMAC-SHA256"
	service   = "s3"

	HdrAuthorization  = "Authorization"
	HdrAmzDate        = "X-Amz-Date"
	HdrAmzContentSHA  = "X-Amz-Content-Sha256"
	UnsignedPayload   = "UNSIGNED-PAYLOAD"
	StreamingPayload  = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
)

// Credentials holds the access/secret pair recognized by the verifier.
type Credentials struct {
	AccessKey string
	SecretKey string
}

// ErrUnauthorized is returned for any signature mismatch or missing/malformed credentials.
var ErrUnauthorized = errors.New("signature verification failed")

// Verify checks the SigV4 signature on r against creds and region. It returns
// nil if the request is properly signed, ErrUnauthorized (wrapped with a
// diagnostic) otherwise. On success, r is untouched.
func Verify(r *http.Request, creds Credentials, region string) error {
	if v := r.URL.Query().Get("X-Amz-Algorithm"); v != "" {
		return verifyPresigned(r, creds, region)
	}
	return verifyHeader(r, creds, region)
}

// ---- header form ----

type authParts struct {
	credential    string
	signedHeaders []string
	signature     string
}

func parseAuthHeader(h string) (authParts, error) {
	var p authParts
	if !strings.HasPrefix(h, algorithm+" ") {
		return p, fmt.Errorf("auth header missing %s prefix", algorithm)
	}
	rest := strings.TrimSpace(strings.TrimPrefix(h, algorithm))
	for _, kv := range strings.Split(rest, ",") {
		kv = strings.TrimSpace(kv)
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			continue
		}
		k := strings.TrimSpace(kv[:eq])
		v := strings.TrimSpace(kv[eq+1:])
		switch k {
		case "Credential":
			p.credential = v
		case "SignedHeaders":
			p.signedHeaders = strings.Split(v, ";")
		case "Signature":
			p.signature = v
		}
	}
	if p.credential == "" || len(p.signedHeaders) == 0 || p.signature == "" {
		return p, errors.New("auth header missing required components")
	}
	return p, nil
}

func verifyHeader(r *http.Request, creds Credentials, region string) error {
	authHdr := r.Header.Get(HdrAuthorization)
	if authHdr == "" {
		return fmt.Errorf("%w: missing Authorization header", ErrUnauthorized)
	}
	parts, err := parseAuthHeader(authHdr)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}

	credBits := strings.Split(parts.credential, "/")
	if len(credBits) != 5 {
		return fmt.Errorf("%w: bad Credential scope", ErrUnauthorized)
	}
	if credBits[0] != creds.AccessKey {
		return fmt.Errorf("%w: unknown access key", ErrUnauthorized)
	}
	dateStamp := credBits[1]
	credRegion := credBits[2]
	credService := credBits[3]
	if credBits[4] != "aws4_request" {
		return fmt.Errorf("%w: bad credential terminator", ErrUnauthorized)
	}
	if credService != service {
		return fmt.Errorf("%w: wrong service %q", ErrUnauthorized, credService)
	}
	if region != "" && credRegion != region {
		// Don't hard-reject; a test server is permissive about region.
		// Use the request's declared region in the signing key instead.
	}

	amzDate := r.Header.Get(HdrAmzDate)
	if amzDate == "" {
		amzDate = r.Header.Get("Date")
	}
	if amzDate == "" {
		return fmt.Errorf("%w: missing X-Amz-Date", ErrUnauthorized)
	}

	payloadHash := r.Header.Get(HdrAmzContentSHA)
	if payloadHash == "" {
		payloadHash = UnsignedPayload
	}

	canonReq := canonicalRequest(r, parts.signedHeaders, payloadHash, false)
	scope := dateStamp + "/" + credRegion + "/" + service + "/aws4_request"
	sts := stringToSign(amzDate, scope, canonReq)
	want := sign(creds.SecretKey, dateStamp, credRegion, service, sts)

	if !hmac.Equal([]byte(want), []byte(parts.signature)) {
		return fmt.Errorf("%w: signature mismatch", ErrUnauthorized)
	}
	return nil
}

// ---- presigned form ----

func verifyPresigned(r *http.Request, creds Credentials, region string) error {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != algorithm {
		return fmt.Errorf("%w: bad algorithm", ErrUnauthorized)
	}
	cred := q.Get("X-Amz-Credential")
	credBits := strings.Split(cred, "/")
	if len(credBits) != 5 {
		return fmt.Errorf("%w: bad presigned Credential", ErrUnauthorized)
	}
	if credBits[0] != creds.AccessKey {
		return fmt.Errorf("%w: unknown access key", ErrUnauthorized)
	}
	dateStamp := credBits[1]
	credRegion := credBits[2]

	amzDate := q.Get("X-Amz-Date")
	expires := q.Get("X-Amz-Expires")
	signedHeaders := strings.Split(q.Get("X-Amz-SignedHeaders"), ";")
	provided := q.Get("X-Amz-Signature")
	if amzDate == "" || expires == "" || provided == "" {
		return fmt.Errorf("%w: missing presigned params", ErrUnauthorized)
	}

	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return fmt.Errorf("%w: bad X-Amz-Date %q", ErrUnauthorized, amzDate)
	}
	expSec := 0
	fmt.Sscanf(expires, "%d", &expSec)
	if expSec > 0 {
		if time.Now().UTC().After(t.Add(time.Duration(expSec) * time.Second)) {
			return fmt.Errorf("%w: presigned URL expired", ErrUnauthorized)
		}
	}

	payloadHash := r.Header.Get(HdrAmzContentSHA)
	if payloadHash == "" {
		payloadHash = UnsignedPayload
	}

	canonReq := canonicalRequest(r, signedHeaders, payloadHash, true)
	scope := dateStamp + "/" + credRegion + "/" + service + "/aws4_request"
	sts := stringToSign(amzDate, scope, canonReq)
	want := sign(creds.SecretKey, dateStamp, credRegion, service, sts)

	if !hmac.Equal([]byte(want), []byte(provided)) {
		return fmt.Errorf("%w: presigned signature mismatch", ErrUnauthorized)
	}
	return nil
}

// ---- canonicalization ----

func canonicalRequest(r *http.Request, signedHeaders []string, payloadHash string, presigned bool) string {
	method := r.Method
	canonURI := canonicalURI(r.URL)
	canonQuery := canonicalQuery(r.URL, presigned)
	canonHeaders, signedJoin := canonicalHeaders(r, signedHeaders)
	return method + "\n" +
		canonURI + "\n" +
		canonQuery + "\n" +
		canonHeaders + "\n" +
		signedJoin + "\n" +
		payloadHash
}

func canonicalURI(u *url.URL) string {
	p := u.EscapedPath()
	if p == "" {
		return "/"
	}
	return p
}

func canonicalQuery(u *url.URL, presigned bool) string {
	q := u.Query()
	keys := make([]string, 0, len(q))
	for k := range q {
		if presigned && k == "X-Amz-Signature" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte('&')
		}
		vs := q[k]
		sort.Strings(vs)
		for j, v := range vs {
			if j > 0 {
				sb.WriteByte('&')
			}
			sb.WriteString(uriEncode(k, false))
			sb.WriteByte('=')
			sb.WriteString(uriEncode(v, false))
		}
	}
	return sb.String()
}

func canonicalHeaders(r *http.Request, signed []string) (string, string) {
	lower := make([]string, len(signed))
	for i, s := range signed {
		lower[i] = strings.ToLower(strings.TrimSpace(s))
	}
	sort.Strings(lower)
	var sb strings.Builder
	for _, h := range lower {
		var v string
		switch h {
		case "host":
			v = r.Host
			if v == "" {
				v = r.Header.Get("Host")
			}
		default:
			vs := r.Header.Values(http.CanonicalHeaderKey(h))
			joined := make([]string, len(vs))
			for i, x := range vs {
				joined[i] = strings.TrimSpace(collapseWS(x))
			}
			v = strings.Join(joined, ",")
		}
		sb.WriteString(h)
		sb.WriteByte(':')
		sb.WriteString(v)
		sb.WriteByte('\n')
	}
	return sb.String(), strings.Join(lower, ";")
}

func collapseWS(s string) string {
	var b strings.Builder
	inQuote := false
	prevSpace := false
	for _, r := range s {
		if r == '"' {
			inQuote = !inQuote
			b.WriteRune(r)
			prevSpace = false
			continue
		}
		if !inQuote && (r == ' ' || r == '\t') {
			if !prevSpace {
				b.WriteByte(' ')
				prevSpace = true
			}
			continue
		}
		b.WriteRune(r)
		prevSpace = false
	}
	return b.String()
}

func stringToSign(amzDate, scope, canonReq string) string {
	sum := sha256.Sum256([]byte(canonReq))
	return algorithm + "\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
}

func sign(secret, dateStamp, region, svc, sts string) string {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(svc))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return hex.EncodeToString(hmacSHA256(kSigning, []byte(sts)))
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

// uriEncode follows AWS SigV4 rules: encode every byte that is not in the
// unreserved set [A-Za-z0-9-_.~]. Optionally treat '/' as unreserved (used
// for canonical URIs but NOT for query/value encoding).
func uriEncode(s string, isPath bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && isPath:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
