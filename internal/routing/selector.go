package routing

import (
	"ai-gateway/internal/domain"
	"sort"
	"strings"
	"sync/atomic"
)

// Snapshot is immutable after construction and safe for lock-free reads.
type Snapshot struct {
	Routes   map[string][]domain.Route
	counters map[string]*atomic.Uint64
	any      []domain.Route
	anyCount atomic.Uint64
	Version  uint64
}

func Build(routes []domain.Route, version uint64) *Snapshot {
	s := &Snapshot{Routes: map[string][]domain.Route{}, counters: map[string]*atomic.Uint64{}, Version: version}
	for _, r := range routes {
		k := strings.ToLower(r.Model.LogicalName)
		s.Routes[k] = append(s.Routes[k], r)
	}
	for k := range s.Routes {
		sort.Slice(s.Routes[k], func(i, j int) bool { return s.Routes[k][i].Model.ID < s.Routes[k][j].Model.ID })
		s.counters[k] = &atomic.Uint64{}
		s.any = append(s.any, s.Routes[k]...)
	}
	sort.Slice(s.any, func(i, j int) bool { return s.any[i].Model.ID < s.any[j].Model.ID })
	return s
}
func (s *Snapshot) Select(logical string) (domain.Route, bool) {
	rs := s.Routes[strings.ToLower(logical)]
	if len(rs) == 0 {
		return domain.Route{}, false
	}
	i := s.counters[strings.ToLower(logical)].Add(1) - 1
	return rs[i%uint64(len(rs))], true
}
func (s *Snapshot) Default() (domain.Route, bool) {
	if len(s.any) == 0 {
		return domain.Route{}, false
	}
	i := s.anyCount.Add(1) - 1
	return s.any[i%uint64(len(s.any))], true
}
