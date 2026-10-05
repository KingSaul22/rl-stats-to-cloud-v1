# RL Stats to Cloud

A Rust workspace designed for reliable, high-performance Rocket League telemetry ingestion over a multi-lane pipeline. It synchronizes live state, event feeds, historical match data, and cumulative team statistics to Firebase — paired with a Tauri v2 desktop UI for real-time monitoring and configuration.

## The future

This project will be archived as a new revised version is being developed. When the times comes, this README will be updated with a link to the new version.

## Status

**Feature-complete (TFG Release).** The core daemon architecture is stable and focuses on:

- **Zero-Data-Loss Ingestion:** Infinite retries with exponential backoff for historical data.
- **Fail-Fast Self-Healing:** Independent actor monitoring that safely tears down and rebuilds the IPC/Network sessions if internal deadlocks or closed channels are detected.
- **Database-Driven Roster Resolution:** Real-time matching of in-game players to registered eSports teams using a strict majority-rule algorithm.
- **NoSQL Read Optimization:** Edge-computed aggregations and denormalized indexes to minimize downstream (Mobile/Frontend) read costs and latency.

## Architecture

```text
Game Source (ws://127.0.0.1:49123)
        │
        ▼
┌─────────────────────────┐
│  rl_stats_core          │  ◄── TCP control ──  rl-stats-to-cloud (Tauri)
│  Daemon                 │       43210            │
│                         │                        │
│  ┌──────────────────┐   │  WebSocket (54321)     │
│  │ UI Sync Server   │───┼──────────────────────► │
│  └──────────────────┘   │   state updates        │
│                         │                        ▼
│  ┌──────────────────┐   │               ┌──────────────┐
│  │ Worker           │───┼──────────────►│  Firebase    │
│  └──────────────────┘   │   HTTP/PUT    │  Realtime DB │
└─────────────────────────┘               └──────────────┘
```

## Quick Start

```bash
# Start the Daemon
cargo run -p rl_stats_core

# In a separate terminal, launch the Tauri desktop app
bun run tauri dev

```

## Workspace Layout

```text
Cargo workspace (Rust edition 2024)
├── core/         rl_stats_core — Daemon binary + library
└── src-tauri/    rl-stats-to-cloud — Tauri v2 desktop client

Frontend: Bun + Vite 6 + TypeScript 6 + Zod 4 (SPA, WebView-hosted)
```

## Reliability Model & Data Plane

| Lane | Channel Type | Delivery Semantics | Backpressure Behaviour |
| --- | --- | --- | --- |
| **LiveState** | `watch` (single value) | At-most-once, deduplicated | Latest wins (coalesced); overwrites stale states |
| **EventFeed** | `mpsc` (bounded, 2048) | At-most-once, best-effort | Drops when full to prevent memory bloat |
| **Historical** | `mpsc` (bounded, 8192) | At-least-once, lossless | Infinite retry with exponential backoff (full jitter) |

**Cross-Lane Synchronization:** The pipeline enforces strict flush barriers. Live states and event feeds are cleanly separated from state-compaction routines (e.g., during match transitions), guaranteeing atomic transitions without ghost matches or data bleed.

See [docs/pipeline.md](docs/pipeline.md) for the full data plane specification.

## Documentation Highlights

The architecture is heavily documented. Below are key resources and Architecture Decision Records (ADRs):

* **[Architecture Overview](docs/architecture.md)** — service boundaries, IPC, ports, configuration.
* **[Data Pipeline](docs/pipeline.md)** — ingestion, classification, sink actors, retry policies.
* **[ADR 0001: Three-Lane Pipeline](docs/decisions/0001-three-lane-pipeline.md)** — Core telemetry splitting logic.
* **[ADR 0020: Strict Cross-Lane Barriers](docs/decisions/0020-strict-cross-lane-synchronization-barriers.md)** — Prevention of engine memory bleed during podiums.
* **[ADR 0022: Majority Rule Roster Resolution](docs/decisions/0022-majority-rule-roster-resolution.md)** — Real-time team identity matching.
* **[ADR 0023: Denormalized Team Stats in Matches Index](docs/decisions/0023-denormalized-team-stats-in-matches-index.md)** — NoSQL optimization for mobile client progression charts.

*A complete list of 23 ADRs is available in the `docs/decisions/` directory.*

## Configuration

On the first run, the daemon creates `config.json` in the platform config directory. Use Firebase REST Authentication fields under the `connector` object:

```json
{
        "isHeadless": false,
        "websocketUrl": "ws://127.0.0.1:49123",
        "connector": {
                "type": "Firebase",
                "url": "https://<project>.firebaseio.com",
                "apiKey": "<firebase-web-api-key>",
                "email": "<firebase-user-email>",
                "password": "<firebase-user-password>"
        },
        "reconnectDelaySeconds": 5,
        "uiSyncPort": 54321
}
```

A full sample is available in `docs/config.example.json`.
