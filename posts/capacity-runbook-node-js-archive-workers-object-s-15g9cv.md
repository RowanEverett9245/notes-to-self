# Capacity Runbook: Node.js Archive Workers, Object Storage Parts, and Signed Retrieval

**Short answer:** For a large ZIP export, use a worker that streams archive bytes into an object storage multipart upload, records the upload state durably, publishes a short-lived signed download link only after the final object is committed, and expires both abandoned parts and completed exports on purpose.

Do not size this job around the final file alone. The binding constraint is usually the worker's temporary disk, memory, or maximum runtime, while the user-facing SLO includes queue delay, archive production, object commit, and link delivery. A design that survives a 40 GB export but holds 40 GB on local disk has moved the capacity problem; it hasn't solved it.

The operational recommendation is therefore a small state machine, not a heroic request handler: `queued -> uploading -> committed -> available`, with explicit terminal states for cancellation and failure. Keep the HTTP request out of the long-running path. The request should create a job and return an identifier; a worker owns the upload; a separate read path issues the signed link after checking that the object is committed and the caller is still authorized.

## What failure signal changes the design?

Memory pressure is the obvious signal, but it is not the most dangerous one. A ZIP writer emits a byte stream whose final length may not be known in advance, object storage multipart APIs retain uploaded parts until completion or cleanup, and a worker can disappear after sending several parts but before recording that progress. Those three facts create a narrow but important rule: the durable record must identify the job, destination object key, multipart upload identifier, next part number, and completed-part receipts needed for finalization. A retry must consult that record before it writes another byte.

The nasty boundary is ambiguous completion. Suppose the worker sends the last part and loses its lease before it marks the job committed. Starting a second archive under the same key may waste compute or overwrite a valid result, depending on the storage semantics selected by the team. Treat finalization as a reconciliation step: inspect durable job state and the destination object, then either record the existing object as committed or resume/abort the known multipart session. Don't infer success from “all source rows were read.” The storage commit is the success boundary.

Capacity planning should use pessimistic inputs. Estimate uncompressed source bytes, expected compressed bytes as a range rather than a promise, part size, maximum concurrent parts, queue depth, worker memory, temporary disk, and the deadline imposed by the job lease. I'm not sure any universal compression ratio is defensible here; a sample of the actual dataset resolves that uncertainty. Already-compressed media and repetitive text behave very differently, so admission control should rely on a hard source-size ceiling and observed archive ratios, with room for the poor-compression case.

Keep one SLO clock.

If the product promises an export within a given duration, measure from job acceptance to link availability. Breaking the dashboard into queue, generation, upload, and commit spans is useful for diagnosis, but reporting only worker runtime hides overload in the queue. Alert on age of the oldest eligible job, failure rate by state transition, abandoned multipart count, and the gap between committed objects and links successfully delivered.

## How should a Node.js worker move a large ZIP export into object storage?

The safe shape is language-independent even when the production worker is Node.js: create the ZIP as a forward-only stream, buffer only enough bytes to form one storage part, upload parts with bounded concurrency, and complete the multipart session only after every part receipt is durable. The focused Go example below makes the ownership boundaries explicit. `ObjectStore` is deliberately generic; its implementation should call the documented multipart operations of the selected backend without changing the worker state machine.

```go
package export

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
)

type Part struct {
	Number  int
	Receipt string
}

type ObjectStore interface {
	Begin(ctx context.Context, objectKey string) (uploadID string, err error)
	PutPart(ctx context.Context, uploadID string, number int, body io.Reader, size int64) (receipt string, err error)
	Complete(ctx context.Context, uploadID string, parts []Part) error
	Abort(ctx context.Context, uploadID string) error
}

type File struct {
	Name string
	Open func(context.Context) (io.ReadCloser, error)
}

func UploadZIP(ctx context.Context, store ObjectStore, objectKey string, files []File, partSize int64) (err error) {
	if partSize <= 0 {
		return errors.New("part size must be positive")
	}

	uploadID, err := store.Begin(ctx, objectKey)
	if err != nil {
		return fmt.Errorf("begin multipart upload: %w", err)
	}
	completed := false
	defer func() {
		if !completed {
			_ = store.Abort(context.WithoutCancel(ctx), uploadID)
		}
	}()

	pipeReader, pipeWriter := io.Pipe()
	archiveDone := make(chan error, 1)
	go func() {
		zw := zip.NewWriter(pipeWriter)
		for _, file := range files {
			entry, createErr := zw.Create(file.Name)
			if createErr != nil {
				_ = pipeWriter.CloseWithError(createErr)
				archiveDone <- createErr
				return
			}
			source, openErr := file.Open(ctx)
			if openErr != nil {
				_ = pipeWriter.CloseWithError(openErr)
				archiveDone <- openErr
				return
			}
			_, copyErr := io.Copy(entry, source)
			closeErr := source.Close()
			if copyErr != nil || closeErr != nil {
				streamErr := errors.Join(copyErr, closeErr)
				_ = pipeWriter.CloseWithError(streamErr)
				archiveDone <- streamErr
				return
			}
		}
		closeErr := zw.Close()
		_ = pipeWriter.CloseWithError(closeErr)
		archiveDone <- closeErr
	}()

	parts := make([]Part, 0, 8)
	for number := 1; ; number++ {
		buffer := make([]byte, partSize)
		n, readErr := io.ReadFull(pipeReader, buffer)
		if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
			return fmt.Errorf("read archive stream: %w", readErr)
		}
		if n > 0 {
			receipt, putErr := store.PutPart(ctx, uploadID, number, bytesReader(buffer[:n]), int64(n))
			if putErr != nil {
				return fmt.Errorf("upload part %d: %w", number, putErr)
			}
			parts = append(parts, Part{Number: number, Receipt: receipt})
		}
		if readErr == io.ErrUnexpectedEOF || readErr == io.EOF {
			break
		}
	}
	if archiveErr := <-archiveDone; archiveErr != nil {
		return fmt.Errorf("build zip: %w", archiveErr)
	}
	if err := store.Complete(ctx, uploadID, parts); err != nil {
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	completed = true
	return nil
}

type sliceReader struct {
	data []byte
}

func bytesReader(data []byte) io.Reader {
	return &sliceReader{data: data}
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}
```

This version uploads one part at a time, which makes its peak buffer close to one configured part plus ZIP and source-reader overhead. Parallel uploads can increase throughput, but they multiply in-flight memory and complicate ordering, cancellation, and durable checkpointing. Add concurrency only after a serial implementation misses a measured completion objective. Faster is not free.

There is also an implementation caveat: the sample aborts an incomplete session when its process remains alive long enough to run deferred cleanup. A production queue must additionally reconcile sessions left by process termination, because deferred code cannot run after a hard kill. Persisting every receipt inside the example would bury the core streaming path under repository-specific transaction code, so that responsibility belongs in the worker's durable job adapter, not in an in-memory slice.

## Publish the download capability after commit

Commit first.

A signed download URL is a capability, not proof that the requester should still have access. Generate it only after the object commit is visible to the application, bind it to the intended object and download operation, keep its lifetime aligned with the actual retrieval workflow, and avoid storing the URL as the durable identity of the export. Store the object key and job authorization data; issue a fresh URL after authorization when policy permits.

Authorize again.

This ordering prevents a link from escaping while an archive is partial. It also separates two clocks that teams often conflate — URL validity and object retention. A short-lived link can expire while the object remains available for reauthorization, and an object lifecycle rule can remove old exports regardless of whether an old URL string still exists. Object lifecycle management is a storage control; signed-link issuance is an application control.

Log the job ID, object key, state transition, byte count, part number, and correlation ID. Do not log the signed query string. For client-visible failures, `403` after link expiry should lead the application back through authorization and link issuance, while a missing retained object should produce an explicit “export expired” state rather than silently starting a duplicate export.

## Verification, rollback, and the buy-versus-build boundary

Verification needs to prove content, control flow, and cleanup. In a staging bucket, exercise an empty archive, a single entry larger than one part, many small entries, a cancellation during upload, a worker termination between part receipt and checkpoint, and a retry after ambiguous completion. Confirm that the downloaded ZIP opens, entry names match the manifest, and the recorded byte count matches the committed object metadata. Then verify the negative path: no link is issued before commit, an unauthorized caller cannot mint a link, and an expired export is distinguishable from a temporarily unavailable job.

Rollback is mostly about stopping new work while preserving evidence. Pause queue consumption, leave committed exports readable according to the existing retention policy, and reconcile every nonterminal job against its recorded multipart identifier. Abort uploads proven abandoned; do not delete an object merely because a link-delivery step failed. If a new archive implementation changes entry naming or compression behavior, deploy it behind a job-format version so old retries continue with the code path that created their state.

Storage lifecycle rules are the final backstop. Configure expiration for completed exports and cleanup for incomplete multipart uploads where the backend supports those controls, then monitor that the rules actually reduce retained objects and parts. A lifecycle rule doesn't replace application reconciliation — its delay may be longer than the on-call team can tolerate — but it limits the cost and data-retention tail after missed cleanup.

| Decision | Buy a managed object-storage path when | Build or self-host more of the path when | Operational catch |
| --- | --- | --- | --- |
| Multipart data plane | The team wants storage durability and multipart mechanics outside its on-call scope | Data placement or a specialized storage protocol requires direct control | The application still owns job state, retries, and authorization |
| ZIP generation | Standard archive layout and a worker queue meet the product need | The format requires custom indexing, encryption, or reproducibility controls | Custom archive code expands the test matrix and rollback burden |
| Signed delivery | Backend-native signing fits the access model | A proxy must enforce per-request policy or transform content | Proxying restores control but puts download bandwidth and availability on the service SLO |
| Lifecycle cleanup | Time-based retention is sufficient | Legal hold or dataset-specific retention needs application decisions | Rules must be audited against policy, not assumed correct after creation |

The catch is lock-in at the multipart and signing adapters. Keep the job state machine, archive stream, manifest checks, and authorization policy behind narrow interfaces, but don't pretend providers have identical part limits, signing models, or lifecycle behavior. Stick with a direct provider integration when one backend is an accepted constraint and the small adapter layer would only disguise it; choose a portability layer when migration is a funded requirement with tests, not a diagram.

## Sources

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- https://cloud.google.com/storage/docs
