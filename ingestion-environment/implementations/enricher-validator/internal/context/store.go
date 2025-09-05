package context

import (
	"encoding/json"
	"log"
	"os"

	"github.com/lucaslui/hems/enricher-validator/internal/model"
)

type Store struct {
	generic *model.Enrichment
}

func Load(path string) (*Store, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Printf("[warn] Context store genérico não encontrado (%s); enriquecimento virá vazio. Err: %v", path, err)
		return &Store{generic: nil}, nil
	}
	var e model.Enrichment
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, err
	}
	return &Store{generic: &e}, nil
}

func (s *Store) Get() *model.Enrichment { return s.generic }
