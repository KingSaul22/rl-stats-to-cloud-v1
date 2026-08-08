# Rust Systems Architecture Audit

## Executive Summary

After performing a ruthless structural audit of the `rl-stats-to-cloud` project tree, it is evident that the `rl_stats_core` crate suffers from tight coupling to specific infrastructure (Firebase and Rocket League WebSockets). The frontend `src` directory is currently cluttered with development artifacts. 

The proposed architecture introduces a strict **Hexagonal (Ports and Adapters)** pattern. By defining `ports` (Traits) in the pure `domain`, we can inject any infrastructure `adapters` (Firebase, PostgreSQL, CS2, Valorant) without modifying the core business logic.

## User Review Required

> [!WARNING]
> **Extensive Refactoring:** The proposed file movements will completely shatter the `models.rs` god-file and alter the `rl_stats_core` module tree. Please review the Hexagonal structure below and confirm if you approve of these strict boundaries before execution begins.

## 1. Hexagonal Core (`rl_stats_core`)

**Current Flaws:** 
`firebase.rs`, `connector.rs`, and `models.rs` live in the root `src/` directory alongside `main.rs`, bleeding infrastructure logic directly into the application space.

**Proposed Perfect Structure (Trait-Based Dependency Inversion):**
```text
core/src/
├── domain/                      <- Pure business logic (Game & Sink Agnostic)
│   ├── mod.rs
│   ├── actors.rs                <- Moved from worker/actors.rs
│   ├── aggregation.rs           <- Moved from worker/aggregation.rs
│   ├── transformer.rs           <- Moved from worker/transformer.rs
│   ├── models.rs                <- Pure Game Events (split from root models.rs)
│   └── ports/                   <- [NEW] Dependency Inversion Traits
│       ├── mod.rs
│       ├── connector.rs         <- Trait: Game Data Source (e.g., RocketLeague)
│       └── sink.rs              <- Trait: Data Sink (e.g., Firebase, Postgres)
├── infrastructure/              <- Adapters implementing Domain Ports
│   ├── mod.rs
│   └── adapters/
│       ├── mod.rs
│       ├── rocket_league/       <- Rocket League WS Implementation
│       │   ├── mod.rs
│       │   └── client.rs        <- Moved from connector.rs
│       └── firebase/            <- Firebase Data Sink Implementation
│           ├── mod.rs
│           ├── client.rs        <- Moved from firebase.rs
│           ├── auth.rs          <- Moved from firebase_auth.rs
│           └── dto.rs           <- Firebase DTOs (split from root models.rs)
├── application/                 <- Use cases, context, and orchestration
│   ├── mod.rs
│   ├── config.rs                <- Moved from root config.rs
│   ├── context.rs               <- Moved from worker/context.rs
│   └── daemon/                  <- Moved from root daemon/
│       ├── client.rs
│       ├── control.rs
│       ├── mod.rs
│       ├── protocol.rs
│       └── ui_server.rs
├── lib.rs
└── main.rs
```

## 2. Model Segregation (`models.rs` breakdown)
The `models.rs` god-file will be split along boundary lines:
1. **Domain Models:** Any struct representing a pure game event (e.g., `PlayerScore`, `MatchState`) will be moved to `domain/models.rs`.
2. **Infrastructure DTOs:** Any struct with `#[derive(Serialize)]` specifically tailored for Firebase or the Rocket League WebSocket schema will be moved to `infrastructure/adapters/firebase/dto.rs` or the respective adapter directory.

## 3. Frontend Hygiene (`src/`)

**Current Flaws:** 
The frontend is bloated with scratchpads (`fixe1.md`), backups (`main.ts.backup`), and flat file structures.

**Proposed Clean Structure:**
```text
src/
├── app/                         <- App orchestration
│   └── main.ts
├── shared/                      <- Shared utilities
│   ├── api.ts
│   ├── constants.ts
│   └── schemas.ts
├── ui/                          <- UI Components and logic
│   └── ui.ts
├── styles/                      <- CSS
│   └── styles.css
└── assets/                      <- Static assets
    └── ...

[NEW] docs/notes/                <- Moved from src/
├── fixe1.md
├── fixe2.md
└── fixe3.md

[NEW] .scratch/                  <- Ignored temporary directory
└── main.ts.backup
```

## 4. Tauri Client (`src-tauri/`)

The `src-tauri` directory structure is currently acceptable, properly utilizing `bridge/` and `commands/` directories to separate Tauri IPC from the daemon lifecycle management. No major restructuring is needed here, though we will ensure `bridge/lifecycle.rs` correctly references the newly relocated `application::daemon` components from `rl_stats_core`.

---

## Proposed Changes (Execution Plan)

If approved, the following exact operations will be executed:

### Deletions / Cleanups
- [DELETE] `core/src/worker/` (directory will be removed after contents are relocated to `domain/` and `application/`)

### File Splits
- [SPLIT] `core/src/models.rs` -> `core/src/domain/models.rs` (Game entities) & `core/src/infrastructure/adapters/firebase/dto.rs` (Firebase entities)

### Core Movements
- [NEW] `core/src/domain/ports/` created for `connector.rs` and `sink.rs` traits.
- [MODIFY] Move `core/src/connector.rs` -> `core/src/infrastructure/adapters/rocket_league/client.rs`
- [MODIFY] Move `core/src/firebase.rs` -> `core/src/infrastructure/adapters/firebase/client.rs`
- [MODIFY] Move `core/src/firebase_auth.rs` -> `core/src/infrastructure/adapters/firebase/auth.rs`
- [MODIFY] Move `core/src/worker/actors.rs` -> `core/src/domain/actors.rs`
- [MODIFY] Move `core/src/worker/aggregation.rs` -> `core/src/domain/aggregation.rs`
- [MODIFY] Move `core/src/worker/transformer.rs` -> `core/src/domain/transformer.rs`
- [MODIFY] Move `core/src/worker/context.rs` -> `core/src/application/context.rs`
- [MODIFY] Move `core/src/daemon/*` -> `core/src/application/daemon/*`
- [MODIFY] Move `core/src/config.rs` -> `core/src/application/config.rs`

### Frontend Movements
- [NEW] `docs/notes/` and `.scratch/`
- [MODIFY] Move `src/fixe*.md` -> `docs/notes/`
- [MODIFY] Move `src/main.ts.backup` -> `.scratch/`
- [MODIFY] Move `src/main.ts` -> `src/app/main.ts`
- [MODIFY] Move `src/api.ts`, `src/constants.ts`, `src/schemas.ts` -> `src/shared/`
- [MODIFY] Move `src/ui.ts` -> `src/ui/`
- [MODIFY] Move `src/styles.css` -> `src/styles/`

## Verification Plan

### Automated Verification
- Run `cargo check -p rl_stats_core` iteratively to fix module visibility and import paths.
- Run `cargo clippy --all-targets --all-features` enforcing the strict linting defined in `AGENTS.md`.
- Run `bun run build` to verify frontend TS/Vite imports are correctly resolved.
