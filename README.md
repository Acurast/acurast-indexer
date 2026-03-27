<a id="readme-top"></a>

<!-- PROJECT SHIELDS -->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/acurast/acurast-indexer">
    <img src="dashboard.png" alt="Acurast Indexer" width="800">
  </a>

<h3 align="center">Acurast Indexer</h3>

  <p align="center">
    A high-performance blockchain indexer and API for the Acurast network
    <br />
    <a href="#api-reference"><strong>Explore the API docs »</strong></a>
    <br />
    <br />
    <a href="https://dev.indexer.mainnet.acurast.com">View Demo</a>
    &middot;
    <a href="https://github.com/acurast/acurast-indexer/issues/new?labels=bug&template=bug-report---.md">Report Bug</a>
    &middot;
    <a href="https://github.com/acurast/acurast-indexer/issues/new?labels=enhancement&template=feature-request---.md">Request Feature</a>
  </p>
</div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#api-reference">API Reference</a></li>
    <li><a href="#project-structure">Project Structure</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->
## About The Project

Acurast Indexer is a blockchain indexer and JSON-RPC API designed for the Acurast network. It provides efficient querying of blocks, extrinsics, events, deployments, and commitments with support for complex filtering, pagination, and batch requests.

Key features:
* **Real-time indexing** - Follows finalized blocks and indexes backwards for historical data
  * **Multi-phase processing** - Efficient pipeline for extracting addresses, deployments, and storage data. Processing in phases for extensibility without full reindexing
  * **Storage snapshots** - Tracks on-chain storage changes selectively with configurable retention
* **JSON-RPC 2.0 API** - Full-featured API with batch request support
* Developer frontend to explore API

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Built With

* [![Rust][Rust-badge]][Rust-url]
* [![PostgreSQL][PostgreSQL-badge]][PostgreSQL-url]
* [![Axum][Axum-badge]][Axum-url]
* [![Subxt][Subxt-badge]][Subxt-url]

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- GETTING STARTED -->
## Getting Started

### Prerequisites

* Rust (stable)
* PostgreSQL 14+
* Access to an Acurast archive node

Install the SQLx CLI for database migrations:
```sh
cargo install sqlx-cli
```

### Installation

1. Clone the repo
   ```sh
   git clone https://github.com/acurast/acurast-indexer.git
   cd acurast-indexer
   ```

2. Set up environment variables
   ```sh
   cp .env.example .env
   ```

   Configure the required environment variables in `.env`:

   | Variable | Description | Example |
   |----------|-------------|---------|
   | `DATABASE_URL` | PostgreSQL connection string for sqlx tooling | `postgres://user:pass@localhost:5432/acurast` |
   | `ACURAST_INDEXER__AUTH__API_KEY` | API authentication key | `your-secret-api-key` |

   Additional settings can be overridden via environment variables using the `ACURAST_INDEXER__` prefix with double underscores (`__`) for nested keys, but it's recommended to use configuration files (see next point)
   
3. The default configuration will index _Acurast Mainnet_ with a public, rate-limited node. To adapt, just create a local configuration at `configuration/local.yaml` to override defaults or values defined in `configuration/base.yaml` such as `indexer.archive_nodes`.

4. Run database migrations
   ```sh
   sqlx migrate run
   ```

5. Build the project
   ```sh
   cargo build --release
   ```

6. Prepare SQLx for offline compilation (before committing)
   ```sh
   cargo sqlx prepare
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- USAGE -->
## Usage

Run the indexer and API server:
```sh
./target/release/acurast-indexer run
```

For formatted log output, install and use bunyan:
```sh
cargo install bunyan
./target/release/acurast-indexer run | bunyan
```

The API will be available at `http://localhost:8000/api/v1/rpc`.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- PRODUCTION DEPLOYMENT -->
## Production Deployment

For production deployments, use the provided Docker Compose file:

1. Create your production configuration similar to [the example](configuration/production.yaml) at `./configuration/production.yaml` relative to your production `docker-compose.yaml` file.
2. Configure your `.env` file with the required environment variables
3. Start the services:
   ```sh
   docker-compose -f docker-compose.production.yaml up -d
   ```

The compose file mounts `configuration/production.yaml` into the container and passes `-e production` to load it.

### Tuning Worker Parameters

The dev frontend (served at `/`) displays real-time queue pressures, which helps identify bottlenecks. Key parameters to tune in your configuration:

| Parameter | Description |
|-----------|-------------|
| `num_event_queuers` | **Primary load driver.** Controls how fast events are queued for processing. Higher values increase load on phase workers, RPC nodes, and database. Start low and increase gradually. |
| `num_workers_phases` | Number of workers processing events/extrinsics/epochs through phases |
| `num_workers_backwards` | Workers for historical block indexing |
| `num_workers_finalized` | Workers for finalized block processing |
| `num_conn_phases` | RPC connections for phase workers |
| `num_db_conn_phases` | Database connections for phase workers |

Monitor queue metrics in the frontend dashboard and adjust based on observed pressures. If queues are backing up, reduce `num_event_queuers`. If the system is idle, increase it gradually.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- API REFERENCE -->
## API Reference

All API calls use the JSON-RPC 2.0 format. Live API documentation is available at:
- [Mainnet](https://dev.indexer.mainnet.acurast.com)
- [Canary](https://dev.indexer.canary.acurast.com)

### Available Methods

| Method | Description |
|--------|-------------|
| `getBlock` | Get a single block by hash |
| `getBlocks` | Get blocks with filters (block range, time range, sorting) |
| `getBlocksCount` | Count blocks matching filters |
| `getExtrinsic` | Get a single extrinsic with pallet/method names |
| `getExtrinsics` | Get extrinsics with filters (block range, pallet, method, account, events) |
| `getExtrinsicsCount` | Count extrinsics matching filters |
| `getExtrinsicMetadata` | Get pallet and method name mappings |
| `getExtrinsicAddresses` | Get addresses appearing in extrinsic arguments |
| `getSpecVersion` | Get spec version metadata by spec version or block number |
| `getEvent` | Get a single event by extrinsic ID and index |
| `getEvents` | Get events with filters (pallet, variant, account) |
| `getJobs` | Get job/deployment appearances in events |

### Example: Get Extrinsics

```sh
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8000/api/v1/rpc \
  -d '{
    "jsonrpc": "2.0",
    "method": "getExtrinsics",
    "params": {"limit": 10},
    "id": 1
  }'
```

### Example: Batch Requests

```sh
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8000/api/v1/rpc \
  -d '[
    {"jsonrpc": "2.0", "method": "getExtrinsics", "params": {"limit": 10}, "id": 1},
    {"jsonrpc": "2.0", "method": "getBlocksCount", "params": {}, "id": 2}
  ]'
```

<!-- PROJECT STRUCTURE -->
## Project Structure

```
src/
├── main.rs                 # Application entrypoint
├── server.rs               # HTTP server setup with Axum
├── rpc_server.rs           # JSON-RPC 2.0 implementation
├── config.rs               # Configuration loading
│
├── block_queuing.rs        # Fetches blocks from chain
├── block_processing.rs     # Processes blocks, extracts extrinsics
├── extrinsic_indexing.rs   # Indexes extrinsics and arguments
├── event_indexing.rs       # Indexes events from extrinsics
├── epoch_indexing.rs       # Tracks epoch transitions
├── storage_indexing/       # Storage snapshot indexing
│
├── entities/               # Database models
├── data_extraction.rs      # Address extraction from arguments
├── transformation.rs       # SCALE codec to JSON conversion
└── metadata.rs             # Runtime metadata handling

frontend/                   # React dev UI, served via tower-http
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- ROADMAP -->
## Roadmap

- [x] Block, extrinsic, and event indexing
- [x] JSON-RPC 2.0 API with batch support
- [x] Multi-phase processing pipeline
- [x] Ccommitment tracking (latest only, but with custom filtering/indexes)
- [ ] Deployment tracking (latest only, but with custom filtering/indexes)

See the [open issues](https://github.com/acurast/acurast-indexer/issues) for a full list of proposed features and known issues.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feat/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feat/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTACT -->
## Contact

Acurast - [@AcurastNetwork](https://twitter.com/AcurastNetwork) - [Discord](https://discord.gg/wqgC6b6aKe)

Project Link: [https://github.com/acurast/acurast-indexer](https://github.com/acurast/acurast-indexer)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* [Subxt](https://github.com/paritytech/subxt) - Rust library for interacting with Substrate nodes
* [Axum](https://github.com/tokio-rs/axum) - Web framework for Rust
* [SQLx](https://github.com/launchbadge/sqlx) - Async SQL toolkit for Rust
* [Best-README-Template](https://github.com/othneildrew/Best-README-Template)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
[contributors-shield]: https://img.shields.io/github/contributors/acurast/acurast-indexer.svg?style=for-the-badge
[contributors-url]: https://github.com/acurast/acurast-indexer/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/acurast/acurast-indexer.svg?style=for-the-badge
[forks-url]: https://github.com/acurast/acurast-indexer/network/members
[stars-shield]: https://img.shields.io/github/stars/acurast/acurast-indexer.svg?style=for-the-badge
[stars-url]: https://github.com/acurast/acurast-indexer/stargazers
[issues-shield]: https://img.shields.io/github/issues/acurast/acurast-indexer.svg?style=for-the-badge
[issues-url]: https://github.com/acurast/acurast-indexer/issues
[license-shield]: https://img.shields.io/github/license/acurast/acurast-indexer.svg?style=for-the-badge
[license-url]: https://github.com/acurast/acurast-indexer/blob/main/LICENSE

[Rust-badge]: https://img.shields.io/badge/Rust-000000?style=for-the-badge&logo=rust&logoColor=white
[Rust-url]: https://www.rust-lang.org/
[PostgreSQL-badge]: https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white
[PostgreSQL-url]: https://www.postgresql.org/
[Axum-badge]: https://img.shields.io/badge/Axum-000000?style=for-the-badge&logo=rust&logoColor=white
[Axum-url]: https://github.com/tokio-rs/axum
[Subxt-badge]: https://img.shields.io/badge/Subxt-282828?style=for-the-badge&logo=rust&logoColor=white
[Subxt-url]: https://github.com/paritytech/subxt
