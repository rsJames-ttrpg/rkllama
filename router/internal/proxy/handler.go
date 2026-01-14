package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"time"

	"github.com/rsjames-ttrpg/rkllama/router/internal/discovery"
)

// Handler handles HTTP requests and proxies them to appropriate pods
type Handler struct {
	discovery *discovery.Discovery
}

// NewHandler creates a new proxy handler
func NewHandler(disc *discovery.Discovery) *Handler {
	return &Handler{
		discovery: disc,
	}
}

// Health handles liveness probe
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

// Ready handles readiness probe
func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	if !h.discovery.HasPods() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"no pods discovered"}`))
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"pods":   len(h.discovery.GetAllPods()),
		"models": h.discovery.GetAllModels(),
	})
}

// Proxy handles all other requests
func (h *Handler) Proxy(w http.ResponseWriter, r *http.Request) {
	// Special handling for aggregate endpoints
	if r.URL.Path == "/api/tags" && r.Method == http.MethodGet {
		h.aggregateTags(w, r)
		return
	}
	if r.URL.Path == "/api/ps" && r.Method == http.MethodGet {
		h.aggregatePS(w, r)
		return
	}
	// Router status endpoints
	if r.URL.Path == "/router/status" && r.Method == http.MethodGet {
		h.routerStatus(w, r)
		return
	}
	if r.URL.Path == "/router/pods" && r.Method == http.MethodGet {
		h.routerPods(w, r)
		return
	}
	if r.URL.Path == "/router/models" && r.Method == http.MethodGet {
		h.routerModels(w, r)
		return
	}
	if r.URL.Path == "/router/force_unload_all" && r.Method == http.MethodPost {
		h.forceUnloadAll(w, r)
		return
	}

	// Extract model from request
	model, body, err := h.extractModel(r)
	if err != nil {
		slog.Debug("failed to extract model", "error", err)
		// Fall back to any pod if model extraction fails
		h.proxyToAnyPod(w, r, body)
		return
	}

	if model == "" {
		// No model specified, proxy to any pod
		h.proxyToAnyPod(w, r, body)
		return
	}

	// Resolve model pattern (supports regex matching against loaded models)
	resolvedModel, body, err := h.resolveModelPattern(model, body)
	if err != nil {
		slog.Warn("failed to resolve model pattern", "pattern", model, "error", err)
	}

	// Find an endpoint (pod or external server) with the model loaded
	endpoint := h.discovery.GetNextEndpointForModel(resolvedModel)
	if endpoint == nil {
		slog.Warn("no endpoint found for model", "model", resolvedModel, "original_pattern", model)
		http.Error(w, fmt.Sprintf("model %q not loaded on any endpoint", model), http.StatusServiceUnavailable)
		return
	}

	if endpoint.Pod != nil {
		slog.Debug("routing request to pod", "model", resolvedModel, "pattern", model, "pod", endpoint.Pod.Name, "path", r.URL.Path)
		h.proxyToPod(w, r, endpoint.Pod, body)
	} else if endpoint.ExternalServer != nil {
		slog.Debug("routing request to external server", "model", resolvedModel, "pattern", model, "server", endpoint.ExternalServer.Name, "path", r.URL.Path)
		h.proxyToExternalServer(w, r, endpoint.ExternalServer, body)
	}
}

// extractModel reads the request body and extracts the model field
func (h *Handler) extractModel(r *http.Request) (string, []byte, error) {
	if r.Body == nil {
		return "", nil, nil
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", nil, err
	}
	r.Body.Close()

	if len(body) == 0 {
		return "", body, nil
	}

	// Try to parse as JSON to extract model
	var req struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		// Not JSON or no model field, that's ok
		return "", body, nil
	}

	return req.Model, body, nil
}

// resolveModelPattern attempts to match the model pattern against loaded models.
// If an exact match exists, it returns the model unchanged.
// If the pattern matches a loaded model via regex, it returns the matched model and updated body.
// Returns the resolved model name, updated body (with model replaced), and any error.
func (h *Handler) resolveModelPattern(modelPattern string, body []byte) (string, []byte, error) {
	// First check for exact match
	pods := h.discovery.GetPodsForModel(modelPattern)
	if len(pods) > 0 {
		return modelPattern, body, nil
	}

	// Try to compile as regex
	re, err := regexp.Compile(modelPattern)
	if err != nil {
		// Not a valid regex, return as-is
		return modelPattern, body, nil
	}

	// Match against all loaded models
	allModels := h.discovery.GetAllModels()
	for _, model := range allModels {
		if re.MatchString(model) {
			slog.Debug("regex model match", "pattern", modelPattern, "matched", model)

			// Replace model in body
			newBody, err := h.replaceModelInBody(body, model)
			if err != nil {
				return model, body, err
			}
			return model, newBody, nil
		}
	}

	// No match found, return original
	return modelPattern, body, nil
}

// replaceModelInBody replaces the model field in the JSON body with the new model name
func (h *Handler) replaceModelInBody(body []byte, newModel string) ([]byte, error) {
	if len(body) == 0 {
		return body, nil
	}

	// Parse the body as a generic map to preserve all fields
	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return body, err
	}

	// Replace the model field
	data["model"] = newModel

	// Re-encode
	newBody, err := json.Marshal(data)
	if err != nil {
		return body, err
	}

	return newBody, nil
}

// proxyToAnyPod proxies to any available pod
func (h *Handler) proxyToAnyPod(w http.ResponseWriter, r *http.Request, body []byte) {
	pods := h.discovery.GetAllPods()
	if len(pods) == 0 {
		http.Error(w, "no pods available", http.StatusServiceUnavailable)
		return
	}

	// Simple round-robin across all pods
	pod := pods[0] // TODO: proper round-robin for non-model requests
	h.proxyToPod(w, r, pod, body)
}

// proxyToPod proxies the request to a specific pod
func (h *Handler) proxyToPod(w http.ResponseWriter, r *http.Request, pod *discovery.Pod, body []byte) {
	target := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:8080", pod.IP),
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.Host = target.Host

			// Restore body if we read it
			if body != nil {
				req.Body = io.NopCloser(bytes.NewReader(body))
				req.ContentLength = int64(len(body))
			}
		},
		// Streaming support: don't buffer response
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.Error("proxy error", "pod", pod.Name, "error", err)
			http.Error(w, "proxy error: "+err.Error(), http.StatusBadGateway)
		},
	}

	proxy.ServeHTTP(w, r)
}

// proxyToExternalServer proxies the request to an external server with auth
func (h *Handler) proxyToExternalServer(w http.ResponseWriter, r *http.Request, server *discovery.ExternalServerState, body []byte) {
	target, err := url.Parse(server.URL)
	if err != nil {
		slog.Error("invalid external server URL", "server", server.Name, "url", server.URL, "error", err)
		http.Error(w, "invalid external server URL", http.StatusInternalServerError)
		return
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.Host = target.Host

			// Restore body if we read it
			if body != nil {
				req.Body = io.NopCloser(bytes.NewReader(body))
				req.ContentLength = int64(len(body))
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
		},
		// Intercept response to detect rate limiting
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode == http.StatusTooManyRequests {
				h.discovery.Record429(server.Name)
			}
			return nil
		},
		// Streaming support: don't buffer response
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.Error("proxy error", "server", server.Name, "error", err)
			http.Error(w, "proxy error: "+err.Error(), http.StatusBadGateway)
		},
	}

	proxy.ServeHTTP(w, r)
}

// aggregateTags aggregates /api/tags from all pods and external servers
func (h *Handler) aggregateTags(w http.ResponseWriter, _ *http.Request) {
	pods := h.discovery.GetAllPods()
	externalServers := h.discovery.GetAllExternalServers()

	// Collect unique models across all pods and external servers
	seen := make(map[string]discovery.ModelInfo)
	for _, pod := range pods {
		for _, model := range pod.Models {
			if _, ok := seen[model]; !ok {
				seen[model] = discovery.ModelInfo{
					Name:  model,
					Model: model,
				}
			}
		}
	}
	for _, server := range externalServers {
		if server.Healthy {
			for _, model := range server.Models {
				if _, ok := seen[model]; !ok {
					seen[model] = discovery.ModelInfo{
						Name:  model,
						Model: model,
					}
				}
			}
		}
	}

	models := make([]discovery.ModelInfo, 0, len(seen))
	for _, m := range seen {
		models = append(models, m)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"models": models,
	})
}

// aggregatePS aggregates /api/ps from all pods and external servers
func (h *Handler) aggregatePS(w http.ResponseWriter, _ *http.Request) {
	pods := h.discovery.GetAllPods()
	externalServers := h.discovery.GetAllExternalServers()

	type runningModel struct {
		Name       string `json:"name"`
		Model      string `json:"model"`
		NodeName   string `json:"node_name,omitempty"`
		PodName    string `json:"pod_name,omitempty"`
		ServerName string `json:"server_name,omitempty"`
		External   bool   `json:"external"`
	}

	running := make([]runningModel, 0)
	for _, pod := range pods {
		for _, model := range pod.Models {
			running = append(running, runningModel{
				Name:     model,
				Model:    model,
				NodeName: pod.NodeName,
				PodName:  pod.Name,
				External: false,
			})
		}
	}
	for _, server := range externalServers {
		if server.Healthy {
			for _, model := range server.Models {
				running = append(running, runningModel{
					Name:       model,
					Model:      model,
					ServerName: server.Name,
					External:   true,
				})
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"models": running,
	})
}

// routerStatus returns detailed router status
func (h *Handler) routerStatus(w http.ResponseWriter, _ *http.Request) {
	pods := h.discovery.GetAllPods()
	externalServers := h.discovery.GetAllExternalServers()

	type podStatus struct {
		Name     string   `json:"name"`
		IP       string   `json:"ip"`
		NodeName string   `json:"node_name"`
		Ready    bool     `json:"ready"`
		Models   []string `json:"models"`
	}

	type externalServerStatus struct {
		Name             string   `json:"name"`
		URL              string   `json:"url"`
		Healthy          bool     `json:"healthy"`
		FailureCount     int      `json:"failure_count"`
		Models           []string `json:"models"`
		Weight           int      `json:"weight"`
		RateLimited      bool     `json:"rate_limited"`
		RateLimitedUntil string   `json:"rate_limited_until,omitempty"`
		RateLimit429s    int      `json:"rate_limit_429s"`
	}

	podStatuses := make([]podStatus, 0, len(pods))
	totalModels := 0
	for _, pod := range pods {
		podStatuses = append(podStatuses, podStatus{
			Name:     pod.Name,
			IP:       pod.IP,
			NodeName: pod.NodeName,
			Ready:    pod.Ready,
			Models:   pod.Models,
		})
		totalModels += len(pod.Models)
	}

	externalStatuses := make([]externalServerStatus, 0, len(externalServers))
	for _, server := range externalServers {
		rateLimited := h.discovery.IsServerRateLimited(server.Name)
		rateLimitedUntil := ""
		if !server.RateLimitedUntil.IsZero() && rateLimited {
			rateLimitedUntil = server.RateLimitedUntil.Format(time.RFC3339)
		}

		externalStatuses = append(externalStatuses, externalServerStatus{
			Name:             server.Name,
			URL:              server.URL,
			Healthy:          server.Healthy,
			FailureCount:     server.FailureCount,
			Models:           server.Models,
			Weight:           server.Weight,
			RateLimited:      rateLimited,
			RateLimitedUntil: rateLimitedUntil,
			RateLimit429s:    len(server.RateLimited429s),
		})
		if server.Healthy && !rateLimited {
			totalModels += len(server.Models)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":           "ok",
		"pods":             podStatuses,
		"external_servers": externalStatuses,
		"pod_count":        len(pods),
		"external_count":   len(externalServers),
		"model_count":      len(h.discovery.GetAllModels()),
		"loaded_count":     totalModels,
		"unique_models":    h.discovery.GetAllModels(),
	})
}

// routerPods returns detailed information about all pods and external servers
func (h *Handler) routerPods(w http.ResponseWriter, _ *http.Request) {
	pods := h.discovery.GetAllPods()
	externalServers := h.discovery.GetAllExternalServers()

	type podInfo struct {
		Name       string   `json:"name"`
		IP         string   `json:"ip,omitempty"`
		URL        string   `json:"url,omitempty"`
		NodeName   string   `json:"node_name,omitempty"`
		Ready      bool     `json:"ready"`
		Models     []string `json:"models"`
		ModelCount int      `json:"model_count"`
		External   bool     `json:"external"`
		Weight     int      `json:"weight,omitempty"`
	}

	podInfos := make([]podInfo, 0, len(pods)+len(externalServers))
	for _, pod := range pods {
		podInfos = append(podInfos, podInfo{
			Name:       pod.Name,
			IP:         pod.IP,
			NodeName:   pod.NodeName,
			Ready:      pod.Ready,
			Models:     pod.Models,
			ModelCount: len(pod.Models),
			External:   false,
		})
	}

	for _, server := range externalServers {
		podInfos = append(podInfos, podInfo{
			Name:       server.Name,
			URL:        server.URL,
			Ready:      server.Healthy,
			Models:     server.Models,
			ModelCount: len(server.Models),
			External:   true,
			Weight:     server.Weight,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"pods": podInfos,
	})
}

// routerModels returns models with their pod and external server locations
func (h *Handler) routerModels(w http.ResponseWriter, _ *http.Request) {
	models := h.discovery.GetAllModels()
	externalServers := h.discovery.GetAllExternalServers()

	// Build a map of model -> external servers
	modelToExternalServers := make(map[string][]string)
	for _, server := range externalServers {
		if server.Healthy {
			for _, model := range server.Models {
				modelToExternalServers[model] = append(modelToExternalServers[model], server.Name)
			}
		}
	}

	type modelInfo struct {
		Name            string   `json:"name"`
		Pods            []string `json:"pods"`
		Nodes           []string `json:"nodes"`
		ExternalServers []string `json:"external_servers,omitempty"`
		Replicas        int      `json:"replicas"`
	}

	modelInfos := make([]modelInfo, 0, len(models))
	for _, model := range models {
		pods := h.discovery.GetPodsForModel(model)
		podNames := make([]string, 0, len(pods))
		nodeNames := make([]string, 0, len(pods))
		nodeSet := make(map[string]bool)

		for _, pod := range pods {
			podNames = append(podNames, pod.Name)
			if !nodeSet[pod.NodeName] {
				nodeSet[pod.NodeName] = true
				nodeNames = append(nodeNames, pod.NodeName)
			}
		}

		externalServerNames := modelToExternalServers[model]

		modelInfos = append(modelInfos, modelInfo{
			Name:            model,
			Pods:            podNames,
			Nodes:           nodeNames,
			ExternalServers: externalServerNames,
			Replicas:        len(pods) + len(externalServerNames),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"models": modelInfos,
	})
}

// forceUnloadAll calls /force_unload_all on all pods to kill stuck workers
func (h *Handler) forceUnloadAll(w http.ResponseWriter, r *http.Request) {
	pods := h.discovery.GetAllPods()
	if len(pods) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"message": "No pods available",
			"results": []any{},
		})
		return
	}

	type podResult struct {
		PodName       string   `json:"pod_name"`
		NodeName      string   `json:"node_name"`
		Success       bool     `json:"success"`
		Error         string   `json:"error,omitempty"`
		KilledTracked []string `json:"killed_tracked,omitempty"`
		KilledOrphaned []int   `json:"killed_orphaned,omitempty"`
	}

	results := make([]podResult, 0, len(pods))
	client := &http.Client{Timeout: 60 * time.Second}

	for _, pod := range pods {
		result := podResult{
			PodName:  pod.Name,
			NodeName: pod.NodeName,
		}

		url := fmt.Sprintf("http://%s:8080/force_unload_all", pod.IP)
		req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, url, nil)
		if err != nil {
			result.Error = err.Error()
			results = append(results, result)
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			result.Error = err.Error()
			results = append(results, result)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			result.Error = fmt.Sprintf("unexpected status: %d", resp.StatusCode)
			resp.Body.Close()
			results = append(results, result)
			continue
		}

		var unloadResp struct {
			KilledTracked  []string `json:"killed_tracked"`
			KilledOrphaned []int    `json:"killed_orphaned"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&unloadResp); err != nil {
			result.Error = fmt.Sprintf("failed to decode response: %v", err)
			resp.Body.Close()
			results = append(results, result)
			continue
		}
		resp.Body.Close()

		result.Success = true
		result.KilledTracked = unloadResp.KilledTracked
		result.KilledOrphaned = unloadResp.KilledOrphaned
		results = append(results, result)

		slog.Info("force unloaded pod", "pod", pod.Name, "node", pod.NodeName,
			"killed_tracked", len(unloadResp.KilledTracked),
			"killed_orphaned", len(unloadResp.KilledOrphaned))
	}

	// Count successes
	successCount := 0
	for _, r := range results {
		if r.Success {
			successCount++
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"message":       fmt.Sprintf("Force unload completed on %d/%d pods", successCount, len(pods)),
		"total_pods":    len(pods),
		"success_count": successCount,
		"results":       results,
	})
}
