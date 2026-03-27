package http

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/metrics"
)

type Server struct {
	server   *http.Server
	metrics  *metrics.Metrics
	configMu sync.RWMutex
	config   *Config
}

type Config struct {
	EventsPerSecond int
	Mode            string // regular, peak, night
	Pause           bool
}

func NewServer(port string, m *metrics.Metrics, initialConfig *Config) *Server {
	s := &Server{
		metrics: m,
		config:  initialConfig,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/config", s.handleConfig)
	mux.HandleFunc("/health", s.handleHealth)

	s.server = &http.Server{
		Addr:         port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	return s
}

func (s *Server) Start() error {
	log.Printf("HTTP server starting on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) GetConfig() *Config {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.config
}

func (s *Server) SetConfig(cfg *Config) {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	s.config = cfg
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.metrics.GetStats())
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		s.configMu.RLock()
		defer s.configMu.RUnlock()
		json.NewEncoder(w).Encode(s.config)

	case http.MethodPut, http.MethodPost:
		var newConfig Config
		if err := json.NewDecoder(r.Body).Decode(&newConfig); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.SetConfig(&newConfig)
		log.Printf("Config updated: %+v", newConfig)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
