package reconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/rsjames-ttrpg/rkllama/router/internal/discovery"
	"gopkg.in/yaml.v3"
)

// ModelConfig represents the desired state for a model
type ModelConfig struct {
	// Explicit node placement (optional)
	Nodes []string `yaml:"nodes,omitempty"`

	// Replica-based placement - router picks nodes automatically
	Replicas int `yaml:"replicas,omitempty"`

	// Priority for placement decisions (higher = place first, default 0)
	Priority int `yaml:"priority,omitempty"`
}

// RateLimitConfig configures the circuit breaker for rate limiting
type RateLimitConfig struct {
	Threshold int `yaml:"threshold,omitempty"` // 429s before tripping (default: 3)
	Window    int `yaml:"window,omitempty"`    // seconds to count 429s in (default: 60)
	Cooldown  int `yaml:"cooldown,omitempty"`  // seconds to wait before retrying (default: 60)
}

// ExternalServer represents an external Ollama-compatible server
type ExternalServer struct {
	Name      string              `yaml:"name"`
	URL       string              `yaml:"url"`
	Weight    int                 `yaml:"weight,omitempty"` // default 1, number of slots in round-robin
	RateLimit *RateLimitConfig    `yaml:"rateLimit,omitempty"`
	Auth      *ExternalServerAuth `yaml:"auth,omitempty"`
}

// ExternalServerAuth configures authentication for an external server
type ExternalServerAuth struct {
	Type      string    `yaml:"type"` // "bearer" or "api-key"
	SecretRef SecretRef `yaml:"secretRef"`
}

// SecretRef references a Kubernetes secret
type SecretRef struct {
	Name string `yaml:"name"`
	Key  string `yaml:"key"`
}

// Config represents the model assignment configuration
type Config struct {
	Models          map[string]ModelConfig `yaml:"models"`
	ExternalServers []ExternalServer       `yaml:"externalServers,omitempty"`
}

// ResolvedExternalServer contains an external server config with resolved auth token
type ResolvedExternalServer struct {
	Name   string
	URL    string
	Weight int
	Token  string // resolved from K8s secret
	Auth   *ExternalServerAuth
}

// Reconciler ensures actual model state matches desired state
type Reconciler struct {
	client     *kubernetes.Clientset
	namespace  string
	configMap  string
	discovery  *discovery.Discovery
	httpClient *http.Client
	config     *Config

	mu                      sync.RWMutex
	resolvedExternalServers []ResolvedExternalServer
}

// New creates a new Reconciler
func New(ctx context.Context, namespace, configMap string, disc *discovery.Discovery) (*Reconciler, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		slog.Warn("failed to get in-cluster config, using default", "error", err)
		config = &rest.Config{
			Host: "http://localhost:8001",
		}
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	return &Reconciler{
		client:    client,
		namespace: namespace,
		configMap: configMap,
		discovery: disc,
		httpClient: &http.Client{
			Timeout: 30 * time.Second, // Model loading can be slow
		},
	}, nil
}

// Run starts the reconciliation loop
func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial reconcile
	r.reconcile(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

// reconcile compares desired state with actual state and makes adjustments
func (r *Reconciler) reconcile(ctx context.Context) {
	// Load config from ConfigMap
	if err := r.loadConfig(ctx); err != nil {
		slog.Error("failed to load config", "error", err)
		return
	}

	if r.config == nil || len(r.config.Models) == 0 {
		slog.Debug("no model config defined, skipping reconciliation")
		return
	}

	pods := r.discovery.GetAllPods()
	if len(pods) == 0 {
		slog.Debug("no pods discovered, skipping reconciliation")
		return
	}

	// Build node -> pod mapping
	nodeToPod := make(map[string]*discovery.Pod)
	for _, pod := range pods {
		nodeToPod[pod.NodeName] = pod
	}

	// Sort models by priority (higher first)
	type modelEntry struct {
		name   string
		config ModelConfig
	}
	models := make([]modelEntry, 0, len(r.config.Models))
	for name, cfg := range r.config.Models {
		models = append(models, modelEntry{name, cfg})
	}
	// Simple sort by priority descending
	for i := 0; i < len(models)-1; i++ {
		for j := i + 1; j < len(models); j++ {
			if models[j].config.Priority > models[i].config.Priority {
				models[i], models[j] = models[j], models[i]
			}
		}
	}

	// Track which pods are "claimed" by models during this reconcile
	podClaimed := make(map[string]string) // pod name -> model that claimed it

	// For each model in config, ensure it's loaded on the right pods
	for _, m := range models {
		r.reconcileModel(ctx, m.name, m.config, pods, nodeToPod, podClaimed)
	}
}

// reconcileModel ensures a single model is in the desired state
func (r *Reconciler) reconcileModel(
	ctx context.Context,
	modelName string,
	config ModelConfig,
	allPods []*discovery.Pod,
	nodeToPod map[string]*discovery.Pod,
	podClaimed map[string]string,
) {
	// Find pods that currently have this model
	currentPods := r.discovery.GetPodsForModel(modelName)
	currentPodSet := make(map[string]bool)
	for _, pod := range currentPods {
		currentPodSet[pod.Name] = true
	}

	// Determine desired pods based on config mode
	desiredPods := make(map[string]bool)

	if len(config.Nodes) > 0 {
		// Explicit node placement
		for _, nodeName := range config.Nodes {
			if pod, ok := nodeToPod[nodeName]; ok {
				desiredPods[pod.Name] = true
				podClaimed[pod.Name] = modelName
			}
		}
	} else if config.Replicas > 0 {
		// Replica-based placement - pick pods automatically
		// Prefer pods that already have the model
		for _, pod := range currentPods {
			if len(desiredPods) >= config.Replicas {
				break
			}
			desiredPods[pod.Name] = true
			podClaimed[pod.Name] = modelName
		}

		// If we need more, pick pods with fewest models (load balancing)
		if len(desiredPods) < config.Replicas {
			// Sort pods by number of models (ascending)
			available := make([]*discovery.Pod, 0)
			for _, pod := range allPods {
				if !desiredPods[pod.Name] && podClaimed[pod.Name] == "" {
					available = append(available, pod)
				}
			}
			// Sort by model count
			for i := 0; i < len(available)-1; i++ {
				for j := i + 1; j < len(available); j++ {
					if len(available[j].Models) < len(available[i].Models) {
						available[i], available[j] = available[j], available[i]
					}
				}
			}

			for _, pod := range available {
				if len(desiredPods) >= config.Replicas {
					break
				}
				desiredPods[pod.Name] = true
				podClaimed[pod.Name] = modelName
			}
		}

		// Log if we can't meet replica count
		if len(desiredPods) < config.Replicas {
			slog.Warn("insufficient pods for desired replicas",
				"model", modelName,
				"desired", config.Replicas,
				"available", len(desiredPods),
			)
		}
	}

	// Load model on pods that should have it but don't
	for podName := range desiredPods {
		if !currentPodSet[podName] {
			pod := r.discovery.GetPodByName(podName)
			if pod != nil {
				slog.Info("loading model", "model", modelName, "pod", podName, "node", pod.NodeName)
				if err := r.loadModel(ctx, pod, modelName); err != nil {
					slog.Error("failed to load model", "model", modelName, "pod", podName, "error", err)
				}
			}
		}
	}

	// Unload model from pods that shouldn't have it (only if using explicit nodes)
	// For replica mode, we don't unload - just maintain the count
	if len(config.Nodes) > 0 {
		for _, pod := range currentPods {
			if !desiredPods[pod.Name] {
				slog.Info("unloading model", "model", modelName, "pod", pod.Name, "node", pod.NodeName)
				if err := r.unloadModel(ctx, pod, modelName); err != nil {
					slog.Error("failed to unload model", "model", modelName, "pod", pod.Name, "error", err)
				}
			}
		}
	}
}

// loadConfig reads the ConfigMap and parses model configuration
func (r *Reconciler) loadConfig(ctx context.Context) error {
	cm, err := r.client.CoreV1().ConfigMaps(r.namespace).Get(ctx, r.configMap, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get configmap: %w", err)
	}

	data, ok := cm.Data["models.yaml"]
	if !ok {
		// Try models.yml
		data, ok = cm.Data["models.yml"]
		if !ok {
			slog.Debug("no models.yaml in configmap")
			r.config = nil
			return nil
		}
	}

	var config Config
	if err := yaml.Unmarshal([]byte(data), &config); err != nil {
		return fmt.Errorf("failed to parse models.yaml: %w", err)
	}

	r.config = &config

	// Resolve external server secrets
	if err := r.resolveExternalServers(ctx); err != nil {
		slog.Error("failed to resolve external server secrets", "error", err)
	}

	return nil
}

// resolveExternalServers fetches secrets and builds resolved external server list
func (r *Reconciler) resolveExternalServers(ctx context.Context) error {
	if r.config == nil || len(r.config.ExternalServers) == 0 {
		r.mu.Lock()
		r.resolvedExternalServers = nil
		r.mu.Unlock()
		// Clear external servers in discovery
		r.discovery.SetExternalServers(nil)
		return nil
	}

	resolved := make([]ResolvedExternalServer, 0, len(r.config.ExternalServers))
	discoveryConfigs := make([]discovery.ExternalServerConfig, 0, len(r.config.ExternalServers))

	for _, es := range r.config.ExternalServers {
		server := ResolvedExternalServer{
			Name:   es.Name,
			URL:    es.URL,
			Weight: es.Weight,
			Auth:   es.Auth,
		}

		// Default weight to 1
		if server.Weight <= 0 {
			server.Weight = 1
		}

		// Resolve auth token from secret if configured
		if es.Auth != nil && es.Auth.SecretRef.Name != "" {
			token, err := r.fetchSecretValue(ctx, es.Auth.SecretRef.Name, es.Auth.SecretRef.Key)
			if err != nil {
				slog.Error("failed to fetch secret for external server",
					"server", es.Name,
					"secret", es.Auth.SecretRef.Name,
					"error", err)
				// Continue without token - server may still work for unauthenticated endpoints
			} else {
				server.Token = token
			}
		}

		resolved = append(resolved, server)

		// Build discovery config
		authType := ""
		if es.Auth != nil {
			authType = es.Auth.Type
		}

		// Rate limit settings (with defaults)
		rateLimitThreshold := 3
		rateLimitWindow := 60
		rateLimitCooldown := 60
		if es.RateLimit != nil {
			if es.RateLimit.Threshold > 0 {
				rateLimitThreshold = es.RateLimit.Threshold
			}
			if es.RateLimit.Window > 0 {
				rateLimitWindow = es.RateLimit.Window
			}
			if es.RateLimit.Cooldown > 0 {
				rateLimitCooldown = es.RateLimit.Cooldown
			}
		}

		discoveryConfigs = append(discoveryConfigs, discovery.ExternalServerConfig{
			Name:               es.Name,
			URL:                es.URL,
			Weight:             server.Weight,
			Token:              server.Token,
			AuthType:           authType,
			RateLimitThreshold: rateLimitThreshold,
			RateLimitWindow:    rateLimitWindow,
			RateLimitCooldown:  rateLimitCooldown,
		})

		slog.Debug("resolved external server", "name", es.Name, "url", es.URL, "weight", server.Weight, "has_token", server.Token != "")
	}

	r.mu.Lock()
	r.resolvedExternalServers = resolved
	r.mu.Unlock()

	// Update discovery with external server configs
	r.discovery.SetExternalServers(discoveryConfigs)

	return nil
}

// fetchSecretValue retrieves a specific key from a Kubernetes secret
func (r *Reconciler) fetchSecretValue(ctx context.Context, secretName, key string) (string, error) {
	secret, err := r.client.CoreV1().Secrets(r.namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get secret %s: %w", secretName, err)
	}

	data, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("key %s not found in secret %s", key, secretName)
	}

	return string(data), nil
}

// loadModel calls /api/load on a pod
func (r *Reconciler) loadModel(ctx context.Context, pod *discovery.Pod, model string) error {
	url := fmt.Sprintf("http://%s:8080/api/load", pod.IP)

	body, _ := json.Marshal(map[string]string{
		"model": model,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("load failed with status %d", resp.StatusCode)
	}

	return nil
}

// unloadModel calls /api/unload on a pod
func (r *Reconciler) unloadModel(ctx context.Context, pod *discovery.Pod, model string) error {
	url := fmt.Sprintf("http://%s:8080/api/unload", pod.IP)

	body, _ := json.Marshal(map[string]string{
		"model": model,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("unload failed with status %d", resp.StatusCode)
	}

	return nil
}

// GetConfig returns the current configuration
func (r *Reconciler) GetConfig() *Config {
	return r.config
}

// GetExternalServers returns the resolved external servers with auth tokens
func (r *Reconciler) GetExternalServers() []ResolvedExternalServer {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a copy to avoid race conditions
	result := make([]ResolvedExternalServer, len(r.resolvedExternalServers))
	copy(result, r.resolvedExternalServers)
	return result
}
