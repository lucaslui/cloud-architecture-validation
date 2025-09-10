package parquetw

import (
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// CloseFunc define a função de fechamento do writer.
type CloseFunc func() error

// NewLocalParquetWriter cria um writer Parquet baseado em arquivo local (tmp).
// T é o tipo do schema (ex.: model.EnrichedRecord).
func NewLocalParquetWriter[T any](path string, parallel int64, compression string) (*writer.ParquetWriter, CloseFunc, error) {
	// File writer local (tipo concreto é interno do pacote; não exporte)
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		return nil, nil, err
	}

	// Parquet writer tipado
	pw, err := writer.NewParquetWriter(fw, new(T), parallel)
	if err != nil {
		_ = fw.Close()
		return nil, nil, err
	}

	// Compressão
	switch compression {
	case "ZSTD":
		pw.CompressionType = parquet.CompressionCodec_ZSTD
	case "GZIP":
		pw.CompressionType = parquet.CompressionCodec_GZIP
	default:
		pw.CompressionType = parquet.CompressionCodec_SNAPPY
	}

	// Fecha Parquet e file writer; remove o arquivo local após upload (opcional)
	closeFn := func() error {
		// NÃO remova o arquivo aqui.
		if err := pw.WriteStop(); err != nil {
			_ = fw.Close()
			return err
		}
		return fw.Close()
	}

	return pw, closeFn, nil
}
