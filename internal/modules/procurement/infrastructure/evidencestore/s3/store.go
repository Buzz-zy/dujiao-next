package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/dujiao-next/internal/modules/procurement/contract"
)

const (
	eventPrefix = "events/v1/"
	probeKey    = "_system/probes/conditional-put-v1"
	probeBody   = `{"probe":"s3-if-none-match-v1"}`
)

var (
	ErrEventConflict               = errors.New("evidence event ID conflicts with stored payload")
	ErrConditionalWriteUnsupported = errors.New("S3 target failed If-None-Match conditional create verification")
	ErrIntegrity                   = errors.New("S3 evidence read-back integrity check failed")
	ErrOutcomeUnknown              = errors.New("S3 write outcome is unknown; reconcile using the same EventID")
)

type Config struct {
	Endpoint        string
	Bucket          string
	Region          string
	AddressingStyle string
	Timeout         time.Duration
}

type clientAPI interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type Store struct {
	client  clientAPI
	bucket  string
	timeout time.Duration
}

func New(ctx context.Context, cfg Config) (*Store, error) {
	if strings.TrimSpace(cfg.Bucket) == "" || strings.TrimSpace(cfg.Region) == "" {
		return nil, errors.New("S3 evidence bucket and region are required")
	}
	style := strings.ToLower(strings.TrimSpace(cfg.AddressingStyle))
	if style == "" {
		style = "path"
	}
	if style != "path" && style != "virtual" {
		return nil, fmt.Errorf("unsupported S3 addressing style %q", cfg.AddressingStyle)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	accessKey, secretKey := strings.TrimSpace(os.Getenv("AWS_ACCESS_KEY_ID")), strings.TrimSpace(os.Getenv("AWS_SECRET_ACCESS_KEY"))
	if accessKey == "" || secretKey == "" {
		return nil, errors.New("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables are required")
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, os.Getenv("AWS_SESSION_TOKEN"))),
		awsconfig.WithHTTPClient(&http.Client{Timeout: cfg.Timeout}),
	)
	if err != nil {
		return nil, fmt.Errorf("load S3 client configuration: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = style == "path"
		if strings.TrimSpace(cfg.Endpoint) != "" {
			o.BaseEndpoint = aws.String(strings.TrimRight(cfg.Endpoint, "/"))
		}
	})
	store := &Store{client: client, bucket: cfg.Bucket, timeout: cfg.Timeout}
	if err := store.verifyConditionalCreate(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) verifyConditionalCreate(parent context.Context) error {
	ctx, cancel := s.withTimeout(parent)
	defer cancel()
	body := []byte(probeBody)
	firstErr := s.putConditional(ctx, probeKey, body)
	if firstErr != nil && !isPreconditionFailed(firstErr) && !isConditionalRequestConflict(firstErr) {
		return fmt.Errorf("verify S3 conditional create initial write: %w", firstErr)
	}
	stored, err := s.getRaw(ctx, probeKey)
	if err != nil {
		return fmt.Errorf("verify S3 conditional create probe read: %w", err)
	}
	if !bytes.Equal(stored, body) {
		return fmt.Errorf("%w: probe object differs", ErrConditionalWriteUnsupported)
	}
	secondErr := s.putConditional(ctx, probeKey, body)
	if !isPreconditionFailed(secondErr) {
		if secondErr == nil {
			return ErrConditionalWriteUnsupported
		}
		return fmt.Errorf("%w: duplicate probe request did not produce HTTP 412: %v", ErrConditionalWriteUnsupported, secondErr)
	}
	stored, err = s.getRaw(ctx, probeKey)
	if err != nil || !bytes.Equal(stored, body) {
		return fmt.Errorf("%w: probe object changed or became unreadable: %v", ErrConditionalWriteUnsupported, err)
	}
	return nil
}

func (s *Store) AppendIntent(event contract.EvidenceEvent) error {
	return s.append(event, "trade_intent")
}
func (s *Store) AppendResponse(event contract.EvidenceEvent) error {
	return s.append(event, "trade_response")
}

func (s *Store) append(event contract.EvidenceEvent, kind string) error {
	if err := validateEvent(event, kind); err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := s.withTimeout(context.Background())
	defer cancel()
	err = s.putConditional(ctx, eventKey(event.EventID), payload)
	if err != nil {
		stored, readErr := s.getRaw(ctx, eventKey(event.EventID))
		if readErr != nil {
			return fmt.Errorf("%w: conditional put failed (%v), reconciliation read failed (%v)", ErrOutcomeUnknown, err, readErr)
		}
		return verifySameEvent(payload, stored, event.EventID)
	}
	stored, err := s.getRaw(ctx, eventKey(event.EventID))
	if err != nil {
		return fmt.Errorf("S3 put succeeded but read-back failed for event %q: %w", event.EventID, err)
	}
	if err := verifySameEvent(payload, stored, event.EventID); err != nil {
		return fmt.Errorf("%w: successful write did not match its read-back: %v", ErrIntegrity, err)
	}
	return nil
}

func (s *Store) GetByEventID(eventID string) (*contract.EvidenceEvent, error) {
	if eventID == "" {
		return nil, errors.New("event ID is required")
	}
	ctx, cancel := s.withTimeout(context.Background())
	defer cancel()
	data, err := s.getRaw(ctx, eventKey(eventID))
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var event contract.EvidenceEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("decode S3 evidence %q: %w", eventID, err)
	}
	if event.EventID != eventID || validateEvent(event, event.EventType) != nil {
		return nil, fmt.Errorf("%w: object key and event payload do not match", ErrIntegrity)
	}
	return &event, nil
}

func (s *Store) GetByTraceID(traceID string) ([]contract.EvidenceEvent, error) {
	if traceID == "" {
		return nil, errors.New("trace ID is required")
	}
	ctx, cancel := s.withTimeout(context.Background())
	defer cancel()
	var out []contract.EvidenceEvent
	var token *string
	for {
		page, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(eventPrefix), ContinuationToken: token})
		if err != nil {
			return nil, fmt.Errorf("list S3 evidence: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil || !strings.HasPrefix(*obj.Key, eventPrefix) {
				continue
			}
			data, err := s.getRaw(ctx, *obj.Key)
			if err != nil {
				return nil, fmt.Errorf("read listed S3 evidence %q: %w", *obj.Key, err)
			}
			var event contract.EvidenceEvent
			if err := json.Unmarshal(data, &event); err != nil {
				return nil, fmt.Errorf("decode listed S3 evidence %q: %w", *obj.Key, err)
			}
			if eventKey(event.EventID) != *obj.Key || validateEvent(event, event.EventType) != nil {
				return nil, fmt.Errorf("%w: invalid event in object %q", ErrIntegrity, *obj.Key)
			}
			if event.TraceID == traceID {
				out = append(out, event)
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		if page.NextContinuationToken == nil || (token != nil && *token == *page.NextContinuationToken) {
			return nil, errors.New("S3 list pagination returned a missing or repeated continuation token")
		}
		token = page.NextContinuationToken
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EventID < out[j].EventID })
	return out, nil
}

func (s *Store) ListPending() ([]contract.EvidenceEvent, error) {
	ctx, cancel := s.withTimeout(context.Background())
	defer cancel()
	var out []contract.EvidenceEvent
	var token *string
	for {
		page, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(eventPrefix), ContinuationToken: token})
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			data, err := s.getRaw(ctx, *obj.Key)
			if err != nil {
				return nil, err
			}
			var event contract.EvidenceEvent
			if err := json.Unmarshal(data, &event); err != nil {
				return nil, err
			}
			if eventKey(event.EventID) != *obj.Key || validateEvent(event, event.EventType) != nil {
				return nil, ErrIntegrity
			}
			if event.EventType == "trade_response" {
				out = append(out, event)
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		if page.NextContinuationToken == nil || (token != nil && *token == *page.NextContinuationToken) {
			return nil, errors.New("S3 list pagination returned a missing or repeated continuation token")
		}
		token = page.NextContinuationToken
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EventID < out[j].EventID })
	return out, nil
}

func (s *Store) putConditional(ctx context.Context, key string, body []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentType: aws.String("application/json"), IfNoneMatch: aws.String("*")})
	return err
}
func (s *Store) getRaw(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}
func (s *Store) withTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, s.timeout)
}
func eventKey(id string) string {
	return eventPrefix + base64.RawURLEncoding.EncodeToString([]byte(id))
}
func verifySameEvent(expected, actual []byte, id string) error {
	expectedHash, actualHash := sha256.Sum256(expected), sha256.Sum256(actual)
	if !bytes.Equal(expected, actual) || expectedHash != actualHash {
		return fmt.Errorf("%w: event %q expected sha256 %s, got %s", ErrEventConflict, id, hex.EncodeToString(expectedHash[:]), hex.EncodeToString(actualHash[:]))
	}
	return nil
}
func validateEvent(event contract.EvidenceEvent, kind string) error {
	if (kind != "trade_intent" && kind != "trade_response") || event.EventID == "" || event.ProcurementOrderID == 0 || event.TraceID == "" || event.Supplier == "" || event.EventType != kind || event.SchemaVersion != 1 {
		return fmt.Errorf("invalid evidence event %q", event.EventID)
	}
	return nil
}
func isPreconditionFailed(err error) bool {
	var api smithy.APIError
	if errors.As(err, &api) && (api.ErrorCode() == "PreconditionFailed" || api.ErrorCode() == "412") {
		return true
	}
	var status interface{ HTTPStatusCode() int }
	return errors.As(err, &status) && status.HTTPStatusCode() == 412
}
func isConditionalRequestConflict(err error) bool {
	var api smithy.APIError
	if errors.As(err, &api) && api.ErrorCode() == "ConditionalRequestConflict" {
		return true
	}
	var status interface{ HTTPStatusCode() int }
	return errors.As(err, &status) && status.HTTPStatusCode() == 409
}
func isNotFound(err error) bool {
	var api smithy.APIError
	return errors.As(err, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound")
}
func (s *Store) Close() error { return nil }

var _ contract.EvidenceStore = (*Store)(nil)
var _ contract.IndependentEvidenceSource = (*Store)(nil)

func (*Store) IndependentlyConfirmed() bool { return true }

// ConfirmIndependent replicates the exact locally persisted event; it never constructs or regenerates an event.
type Confirmer struct {
	local  contract.EvidenceStore
	remote *Store
}

func NewConfirmer(local contract.EvidenceStore, remote *Store) (*Confirmer, error) {
	if local == nil || remote == nil {
		return nil, errors.New("local evidence store and S3 store are required")
	}
	return &Confirmer{local: local, remote: remote}, nil
}
func (c *Confirmer) ConfirmIndependent(eventID string) error {
	event, err := c.local.GetByEventID(eventID)
	if err != nil {
		return fmt.Errorf("read local evidence %q: %w", eventID, err)
	}
	if event == nil {
		return fmt.Errorf("local evidence %q not found", eventID)
	}
	switch event.EventType {
	case "trade_intent":
		return c.remote.AppendIntent(*event)
	case "trade_response":
		return c.remote.AppendResponse(*event)
	default:
		return fmt.Errorf("unsupported evidence event type %q", event.EventType)
	}
}

var _ contract.IndependentEvidenceConfirmer = (*Confirmer)(nil)
