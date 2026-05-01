const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:5001";

async function getJson(path) {
  const response = await fetch(`${API_BASE}${path}`);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error || `Request failed: ${response.status}`);
  }
  return data;
}

export function fetchSectors() {
  return getJson("/api/sectors");
}

export function fetchOverview(sector, signal = "pub_zscore") {
  return getJson(`/api/sectors/${sector}/overview?signal=${encodeURIComponent(signal)}`);
}

export function fetchAnalysis(sector, signal) {
  return getJson(`/api/sectors/${sector}/analysis?signal=${encodeURIComponent(signal)}`);
}

export function fetchViralAnalysis(sector) {
  return getJson(`/api/sectors/${sector}/viral-analysis`);
}

export function fetchViralFeed(sector, days = 5) {
  return getJson(`/api/viral?sector=${encodeURIComponent(sector)}&days=${days}`);
}
