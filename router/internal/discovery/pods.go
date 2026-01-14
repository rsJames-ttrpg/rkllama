package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Pod represents a discovered rkllama pod
type Pod struct {
	Name     string
	IP       string
	NodeName string
	Ready    bool
	Models   []string // Models currently loaded on this pod
}

// ExternalServerState represents the runtime state of an external server
type ExternalServerState struct {
	Name         string
	URL          string
	Weight       int
	Token        string // resolved auth token
	AuthType     string // "bearer" or "api-key"
	Models       []string
	Healthy      bool
	FailureCount int
	LastCheck    time.Time

	// Rate limit circuit breaker
	RateLimitThreshold int         // 429s before tripping (default: 3)
	RateLimitWindow    int         // seconds to count 429s in (default: 60)
	RateLimitCooldown  int         // seconds to wait before retrying (default: 60)
	RateLimited429s    []time.Time // timestamps of recent 429s
	RateLimitedUntil   time.Time   // if set, server is rate-limited until this time
}

// ModelEndpoint represents either a Pod or an ExternalServer that can serve a model
type ModelEndpoint struct {
	Pod            *Pod                 // set if this is a K8s pod
	ExternalServer *ExternalServerState // set if this is an external server
}

// Discovery watches rkllama pods and tracks their loaded models
type Discovery struct {
	client        *kubernetes.Clientset
	namespace     string
	labelSelector string
	httpClient    *http.Client

	mu              sync.RWMutex
	pods            map[string]*Pod                   // pod name -> Pod
	externalServers map[string]*ExternalServerState   // server name -> state
	modelMap        map[string][]*ModelEndpoint       // model name -> endpoints (weighted)
	rrIndex         map[string]int                    // round-robin index per model
}

// TagsResponse represents the response from /api/tags
type TagsResponse struct {
	Models []ModelInfo `json:"models"`
}

// ModelInfo represents a model from /api/tags
type ModelInfo struct {
	Name       string `json:"name"`
	Model      string `json:"model"`
	ModifiedAt string `json:"modified_at"`
	Size       int64  `json:"size"`
}

// PsResponse represents the response from /api/ps (loaded models)
type PsResponse struct {
	Models []LoadedModelInfo `json:"models"`
}

// LoadedModelInfo represents a model from /api/ps
type LoadedModelInfo struct {
	Name     string `json:"name"`
	Model    string `json:"model"`
	Size     int64  `json:"size"`
	LoadedAt string `json:"loaded_at"`
}

// New creates a new Discovery instance
func New(ctx context.Context, namespace, labelSelector string) (*Discovery, error) {
	// Create in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		// Fall back to kubeconfig for local development
		slog.Warn("failed to get in-cluster config, using default", "error", err)
		config = &rest.Config{
			Host: "http://localhost:8001", // kubectl proxy
		}
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	return &Discovery{
		client:          client,
		namespace:       namespace,
		labelSelector:   labelSelector,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		pods:            make(map[string]*Pod),
		externalServers: make(map[string]*ExternalServerState),
		modelMap:        make(map[string][]*ModelEndpoint),
		rrIndex:         make(map[string]int),
	}, nil
}

// Run starts the discovery loop
func (d *Discovery) Run(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Initial discovery
	d.DiscoverExternalServers(ctx)
	d.discover(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.DiscoverExternalServers(ctx)
			d.discover(ctx)
		}
	}
}

// discover fetches current pod state and external server state
func (d *Discovery) discover(ctx context.Context) {
	selector, err := labels.Parse(d.labelSelector)
	if err != nil {
		slog.Error("invalid label selector", "selector", d.labelSelector, "error", err)
		return
	}

	pods, err := d.client.CoreV1().Pods(d.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		slog.Error("failed to list pods", "error", err)
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Reset pod state
	newPods := make(map[string]*Pod)
	newModelMap := make(map[string][]*ModelEndpoint)

	for _, p := range pods.Items {
		if p.Status.Phase != corev1.PodRunning {
			continue
		}

		pod := &Pod{
			Name:     p.Name,
			IP:       p.Status.PodIP,
			NodeName: p.Spec.NodeName,
			Ready:    isPodReady(&p),
		}

		if pod.IP == "" || !pod.Ready {
			continue
		}

		// Fetch models from this pod
		models, err := d.fetchModels(ctx, pod.IP)
		if err != nil {
			slog.Debug("failed to fetch models from pod", "pod", pod.Name, "error", err)
			continue
		}
		pod.Models = models

		newPods[pod.Name] = pod

		// Update model map - each pod gets 1 slot
		for _, model := range models {
			newModelMap[model] = append(newModelMap[model], &ModelEndpoint{Pod: pod})
		}
	}

	// Add external servers to model map (with weighting)
	for _, es := range d.externalServers {
		if !es.Healthy {
			continue
		}
		// Skip rate-limited servers
		if d.isServerRateLimited(es) {
			continue
		}
		for _, model := range es.Models {
			// Add server multiple times based on weight
			for i := 0; i < es.Weight; i++ {
				newModelMap[model] = append(newModelMap[model], &ModelEndpoint{ExternalServer: es})
			}
		}
	}

	d.pods = newPods
	d.modelMap = newModelMap

	slog.Debug("discovery complete",
		"pods", len(d.pods),
		"external_servers", len(d.externalServers),
		"models", len(d.modelMap),
	)
}

// fetchModels gets the list of loaded models from a pod's /api/ps endpoint
func (d *Discovery) fetchModels(ctx context.Context, podIP string) ([]string, error) {
	url := fmt.Sprintf("http://%s:8080/api/ps", podIP)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var ps PsResponse
	if err := json.NewDecoder(resp.Body).Decode(&ps); err != nil {
		return nil, err
	}

	models := make([]string, 0, len(ps.Models))
	for _, m := range ps.Models {
		models = append(models, m.Name)
	}

	return models, nil
}

// GetPodsForModel returns pods that have the specified model loaded
func (d *Discovery) GetPodsForModel(model string) []*Pod {
	d.mu.RLock()
	defer d.mu.RUnlock()

	endpoints := d.modelMap[model]
	pods := make([]*Pod, 0)
	seen := make(map[string]bool)
	for _, ep := range endpoints {
		if ep.Pod != nil && !seen[ep.Pod.Name] {
			pods = append(pods, ep.Pod)
			seen[ep.Pod.Name] = true
		}
	}
	return pods
}

// GetNextEndpointForModel returns the next endpoint (pod or external server) for round-robin load balancing
func (d *Discovery) GetNextEndpointForModel(model string) *ModelEndpoint {
	d.mu.Lock()
	defer d.mu.Unlock()

	endpoints := d.modelMap[model]
	if len(endpoints) == 0 {
		return nil
	}

	idx := d.rrIndex[model] % len(endpoints)
	d.rrIndex[model] = idx + 1

	return endpoints[idx]
}

// GetNextPodForModel returns the next pod for round-robin load balancing (legacy, pods only)
func (d *Discovery) GetNextPodForModel(model string) *Pod {
	endpoint := d.GetNextEndpointForModel(model)
	if endpoint == nil {
		return nil
	}
	// If it's an external server, keep trying until we get a pod or exhaust options
	// This maintains backward compatibility but prefers the new GetNextEndpointForModel
	if endpoint.Pod != nil {
		return endpoint.Pod
	}
	return nil
}

// GetAllPods returns all discovered pods
func (d *Discovery) GetAllPods() []*Pod {
	d.mu.RLock()
	defer d.mu.RUnlock()

	pods := make([]*Pod, 0, len(d.pods))
	for _, p := range d.pods {
		pods = append(pods, p)
	}
	return pods
}

// GetAllModels returns all known models across all pods
func (d *Discovery) GetAllModels() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	models := make([]string, 0, len(d.modelMap))
	for model := range d.modelMap {
		models = append(models, model)
	}
	return models
}

// HasPods returns true if at least one pod is discovered
func (d *Discovery) HasPods() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.pods) > 0
}

// GetPodByName returns a specific pod by name
func (d *Discovery) GetPodByName(name string) *Pod {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.pods[name]
}

// isPodReady checks if a pod has the Ready condition
func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// ExternalServerConfig is used to update external server configuration
type ExternalServerConfig struct {
	Name               string
	URL                string
	Weight             int
	Token              string
	AuthType           string // "bearer" or "api-key"
	RateLimitThreshold int    // 429s before tripping (default: 3)
	RateLimitWindow    int    // seconds to count 429s in (default: 60)
	RateLimitCooldown  int    // seconds to wait before retrying (default: 60)
}

// SetExternalServers updates the external server configuration
func (d *Discovery) SetExternalServers(servers []ExternalServerConfig) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Preserve existing state for servers that remain
	newServers := make(map[string]*ExternalServerState)
	for _, cfg := range servers {
		// Apply defaults for rate limiting
		threshold := cfg.RateLimitThreshold
		if threshold <= 0 {
			threshold = 3
		}
		window := cfg.RateLimitWindow
		if window <= 0 {
			window = 60
		}
		cooldown := cfg.RateLimitCooldown
		if cooldown <= 0 {
			cooldown = 60
		}

		if existing, ok := d.externalServers[cfg.Name]; ok {
			// Update config but preserve health state and rate limit history
			existing.URL = cfg.URL
			existing.Weight = cfg.Weight
			existing.Token = cfg.Token
			existing.AuthType = cfg.AuthType
			existing.RateLimitThreshold = threshold
			existing.RateLimitWindow = window
			existing.RateLimitCooldown = cooldown
			newServers[cfg.Name] = existing
		} else {
			// New server - start as unhealthy until first successful probe
			newServers[cfg.Name] = &ExternalServerState{
				Name:               cfg.Name,
				URL:                cfg.URL,
				Weight:             cfg.Weight,
				Token:              cfg.Token,
				AuthType:           cfg.AuthType,
				Healthy:            false,
				FailureCount:       0,
				RateLimitThreshold: threshold,
				RateLimitWindow:    window,
				RateLimitCooldown:  cooldown,
				RateLimited429s:    make([]time.Time, 0),
			}
		}
	}
	d.externalServers = newServers
}

// DiscoverExternalServers polls external servers for their models
func (d *Discovery) DiscoverExternalServers(ctx context.Context) {
	d.mu.RLock()
	servers := make([]*ExternalServerState, 0, len(d.externalServers))
	for _, s := range d.externalServers {
		servers = append(servers, s)
	}
	d.mu.RUnlock()

	for _, server := range servers {
		d.probeExternalServer(ctx, server)
	}
}

// probeExternalServer checks health and fetches models from an external server
func (d *Discovery) probeExternalServer(ctx context.Context, server *ExternalServerState) {
	models, err := d.fetchExternalServerModels(ctx, server)

	d.mu.Lock()
	defer d.mu.Unlock()

	server.LastCheck = time.Now()

	if err != nil {
		server.FailureCount++
		slog.Debug("external server probe failed",
			"server", server.Name,
			"url", server.URL,
			"error", err,
			"failure_count", server.FailureCount)

		if server.FailureCount >= 3 {
			if server.Healthy {
				slog.Warn("external server marked unhealthy after 3 failures",
					"server", server.Name,
					"url", server.URL)
			}
			server.Healthy = false
		}
		return
	}

	// Success - reset failure count and mark healthy
	if !server.Healthy {
		slog.Info("external server now healthy",
			"server", server.Name,
			"url", server.URL,
			"models", len(models))
	}
	server.FailureCount = 0
	server.Healthy = true
	server.Models = models
}

// fetchExternalServerModels fetches models from an external server's /api/tags endpoint
func (d *Discovery) fetchExternalServerModels(ctx context.Context, server *ExternalServerState) ([]string, error) {
	url := server.URL + "/api/tags"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	// Add auth header if configured
	if server.Token != "" {
		switch server.AuthType {
		case "bearer":
			req.Header.Set("Authorization", "Bearer "+server.Token)
		case "api-key":
			req.Header.Set("X-API-Key", server.Token)
		}
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var tags TagsResponse
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		return nil, err
	}

	models := make([]string, 0, len(tags.Models))
	for _, m := range tags.Models {
		models = append(models, m.Name)
	}

	return models, nil
}

// GetAllExternalServers returns all external servers
func (d *Discovery) GetAllExternalServers() []*ExternalServerState {
	d.mu.RLock()
	defer d.mu.RUnlock()

	servers := make([]*ExternalServerState, 0, len(d.externalServers))
	for _, s := range d.externalServers {
		servers = append(servers, s)
	}
	return servers
}

// HasEndpoints returns true if at least one pod or healthy external server exists
func (d *Discovery) HasEndpoints() bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.pods) > 0 {
		return true
	}
	for _, s := range d.externalServers {
		if s.Healthy && !d.isServerRateLimited(s) {
			return true
		}
	}
	return false
}

// Record429 records a 429 response from an external server and triggers circuit breaker if threshold exceeded
func (d *Discovery) Record429(serverName string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	server, ok := d.externalServers[serverName]
	if !ok {
		return
	}

	now := time.Now()

	// Add current 429 to the list
	server.RateLimited429s = append(server.RateLimited429s, now)

	// Clean up old entries outside the window
	windowStart := now.Add(-time.Duration(server.RateLimitWindow) * time.Second)
	filtered := make([]time.Time, 0, len(server.RateLimited429s))
	for _, t := range server.RateLimited429s {
		if t.After(windowStart) {
			filtered = append(filtered, t)
		}
	}
	server.RateLimited429s = filtered

	// Check if threshold exceeded
	if len(server.RateLimited429s) >= server.RateLimitThreshold {
		server.RateLimitedUntil = now.Add(time.Duration(server.RateLimitCooldown) * time.Second)
		slog.Warn("external server rate limited - circuit breaker tripped",
			"server", server.Name,
			"url", server.URL,
			"429_count", len(server.RateLimited429s),
			"threshold", server.RateLimitThreshold,
			"cooldown_seconds", server.RateLimitCooldown,
			"until", server.RateLimitedUntil)
	} else {
		slog.Debug("external server returned 429",
			"server", server.Name,
			"429_count", len(server.RateLimited429s),
			"threshold", server.RateLimitThreshold)
	}
}

// isServerRateLimited checks if a server is currently rate limited (must hold lock)
func (d *Discovery) isServerRateLimited(server *ExternalServerState) bool {
	if server.RateLimitedUntil.IsZero() {
		return false
	}
	if time.Now().After(server.RateLimitedUntil) {
		// Cooldown expired, clear rate limit state
		server.RateLimitedUntil = time.Time{}
		server.RateLimited429s = make([]time.Time, 0)
		slog.Info("external server rate limit cooldown expired",
			"server", server.Name,
			"url", server.URL)
		return false
	}
	return true
}

// IsServerRateLimited checks if a server is currently rate limited (public, takes lock)
func (d *Discovery) IsServerRateLimited(serverName string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	server, ok := d.externalServers[serverName]
	if !ok {
		return false
	}
	return d.isServerRateLimited(server)
}
