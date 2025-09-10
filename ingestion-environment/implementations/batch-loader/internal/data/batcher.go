package data

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	parquetw "github.com/lucaslui/hems/batch-loader/internal/compression"
	"github.com/lucaslui/hems/batch-loader/internal/model"
	"github.com/lucaslui/hems/batch-loader/internal/storage"
)

type Record = model.OutboundRecord

type Batcher struct {
	MaxRecords  int
	MaxBytes    int64
	MaxInterval time.Duration

	resetTime time.Time
	buf       []Record
	bytes     int64

	S3Client *storage.Client
	S3Base   string
	Compress string
}

func NewBatcher(maxRecords int, maxBytes int64, maxInterval time.Duration, s3c *storage.Client, basePath, compression string) *Batcher {
    b := &Batcher{
        MaxRecords:  maxRecords,
        MaxBytes:    maxBytes,
        MaxInterval: maxInterval,
        S3Client:    s3c,
        S3Base:      basePath,
        Compress:    compression,
        resetTime:   time.Now().UTC(),
    }
    if maxRecords > 0 {
        b.buf = make([]Record, 0, maxRecords) // NOVO: pré-alocação
    }
    return b
}

func (b *Batcher) Flush(ctx context.Context) (int, error) {
	n := len(b.buf)
	if n == 0 {
		return 0, nil
	}

	ts := time.Now().UTC()
	fn := fmt.Sprintf("part-%s-%s.parquet", ts.Format("2006-01-02T15-04-05Z"), uuid.NewString())
	tmp := filepath.Join(os.TempDir(), fn)

	// 1) cria writer parquet (nova assinatura)
	pw, closeFn, err := parquetw.NewLocalParquetWriter[Record](tmp, 4, b.Compress)
	if err != nil {
		return 0, err
	}

	// 2) escreve todos os registros
	for i := range b.buf {
		if err := pw.Write(b.buf[i]); err != nil {
			_ = closeFn() // tenta fechar mesmo com erro
			return 0, err
		}
	}

	// 3) fecha o parquet (garante flush de disco)
	if err := closeFn(); err != nil {
		return 0, err
	}

	// 4) abre o arquivo e faz upload em streaming
	f, err := os.Open(tmp)
	if err != nil {
		return 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return 0, err
	}

	objPath := storage.BuildObjectPath(b.S3Base, ts, fn)
	if err := b.S3Client.Upload(ctx, objPath, f, fi.Size(), "application/octet-stream"); err != nil {
		_ = f.Close()
		return 0, err
	}
	_ = f.Close()

	// 5) limpa o arquivo local
	_ = os.Remove(tmp)

	// 6) reset do buffer
	b.buf = b.buf[:0]
	b.bytes = 0
	b.resetTime = time.Now().UTC()

	return n, nil
}

func ToRecord(evt model.InboundEnvelope) Record {
	return Record{
		DeviceID:      evt.DeviceID,
		DeviceType:    evt.DeviceType,
		EventType:     evt.EventType,
		SchemaVersion: evt.SchemaVersion,
		Timestamp:     model.ToMillis(evt.Timestamp),
		Payload:       string(evt.Payload),

		RegionId:       evt.Context.RegionID,
		DeviceLocation: evt.Context.DeviceLocation,
		DeviceCategory: evt.Context.DeviceCategory,
		DeviceModel:    evt.Context.DeviceModel,
		EndUserID:      evt.Context.EndUserID,
		ContractType:   evt.Context.ContractType,
		Programs:       strings.Join(evt.Context.Programs, ","),

		EventID:        evt.Metadata.EventID,
	}
}

func (b *Batcher) Add(r Record) (shouldFlush bool) {
    if len(b.buf) == 0 {
        b.resetTime = time.Now().UTC()
        b.bytes = 0
    }

    b.buf = append(b.buf, r)
    b.bytes += int64(len(r.Payload)) + 256

    byRecords := b.MaxRecords > 0 && len(b.buf) >= b.MaxRecords
    byBytes   := b.MaxBytes   > 0 && b.bytes    >= b.MaxBytes

    return byRecords || byBytes
}

func (b *Batcher) ShouldFlushByInterval() bool {
    return b.MaxInterval > 0 && len(b.buf) > 0 && time.Since(b.resetTime) >= b.MaxInterval
}
