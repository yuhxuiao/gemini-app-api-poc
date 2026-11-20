# gemini-app-api-poc

macOS local protocol gateway PoC (`Dual-Plane v4.6`) bridging local Gemini sessions to Anthropic Messages (`/v1/messages`) and OpenAI Chat/Responses (`/v1/chat/completions`, `/v1/responses`) APIs over loopback (`127.0.0.1:18896`), with embedded local `stdio` MCP support and NIAH context benchmarking tools.

## Architecture Overview

- **Local HTTP/SSE Gateway (`gateway_poc.py`)**: Exposes Anthropic Messages & OpenAI-compatible endpoints on `127.0.0.1:18896`, with a local status dashboard at `/dashboard`, session cursor management, 192K soft-watermark rollover, and error self-heal.
- **Plane 1 (`dsh_webkit_bridge.swift` + `webkit_plane_client.py`)**: Native macOS bridge communicating over `AF_UNIX` with `2 MB` socket buffers, `POLLOUT` backpressure handling, and strict EOF detection to prevent truncation on large streaming outputs.
- **Plane 2 (`dsh_worker_cli.py` + `dsh_mcp_stdio.py`)**: Persistent single-session daemon mode using POSIX FIFO and local `stdio` MCP (`dsh_mcp_launcher.c`) with automatic pagination for large tool outputs.
- **DSH-TP v1.1 Streaming Parser (`streaming_envelope_parser.py`)**: Real-time `input_json_delta` streaming for raw-mode tool calls, 3-layer JSON recovery, and gateway-internal retry.
- **Embedded Local MCP (`embedded_local_mcp.py`)**: Zero-public-IP local `stdio` MCP host loading `gemini_local_mcp_config.json` or project-level `.mcp.json`.

## Prerequisites

- macOS (Apple Silicon / Intel)
- Xcode Command Line Tools (`swiftc`, `clang`, `codesign`)
- System `/usr/bin/python3` (standard library only)
- Optional: Claude Code CLI

## Quick Start

### 1. Build Native Components & Run 6-Stage Verification

Compiled binaries under `bin/` are excluded from version control. Run the built-in workflow to compile `bin/dsh-mcp-stdio` and `bin/DSHWebKitBridge.app` and run the verification suite:

```bash
chmod +x cc-demo.sh 一键启动-ClaudeCode.command
./cc-demo.sh workflow
```

### 2. Configure Local MCP (Optional)

```bash
cp gemini_local_mcp_config.example.json gemini_local_mcp_config.json
# Update /path/to/gemini-app-api-poc to your local directory path
```

### 3. Run the Gateway

```bash
# Default mode (Plane 1)
./cc-demo.sh

# Plane 2 persistent daemon mode
./cc-demo.sh task-mode

# Check health & status / clean chats / stop
./cc-demo.sh status
./cc-demo.sh clean-chats
./cc-demo.sh stop
```

See [`DEMO_GUIDE.md`](DEMO_GUIDE.md) for additional usage details.

## Repository Layout

| Path | Description |
| :--- | :--- |
| `gateway_poc.py` | Main local HTTP/SSE gateway server |
| `dsh_webkit_bridge.swift` | macOS native WebKit + `AF_UNIX` bridge source |
| `webkit_plane_client.py` | Python `AF_UNIX` streaming client and model router |
| `streaming_envelope_parser.py` | DSH-TP v1.1 streaming envelope state machine |
| `embedded_local_mcp.py` | Embedded local `stdio` MCP host |
| `dsh_mcp_stdio.py` / `dsh_mcp_launcher.c` | Local MCP bridge script and native launcher wrapper |
| `dsh_worker_cli.py` | Plane 2 persistent worker CLI |
| `dsh_local_tools_mcp.py` | Example local system-info MCP server |
| `cc-demo.sh` / `一键启动-ClaudeCode.command` / `dsh_cc_launcher.swift` | Lifecycle scripts and macOS AppKit launcher source |
| `claude_gateway_settings.json` | Claude Code `/model` picker configuration |
| `workflow_runner.py` | 6-stage build, unit, E2E, and resilience test suite |
| `niah_bench/` | NIAH (100K–800K) and multi-turn compaction benchmark generator & scorer |
| `test_e2e_poc.py`, `verify_v2_demo.py`, `deep_protocol_and_stream_test.py`, `benchmark_live_models.py`, `e2e_live_claude_cli.py` | Earlier prototype test and benchmark scripts |

## NIAH Benchmark Suite

Generated corpus files (~12 MB) are gitignored. Regenerate locally with:

```bash
python3 ./niah_bench/generate_niah_suite.py
```

See [`niah_bench/TEST_GUIDE.md`](niah_bench/TEST_GUIDE.md) for instructions.

## License

MIT License — see [`LICENSE`](LICENSE).
