# GenLayer Dashboard

Lightweight dashboard for monitoring a GenLayer node.

It includes a FastAPI backend, a React/Vite frontend, SQLite-based metric history, and support for custom charts stored in JSON.

## Features

- node and validator status
- CPU, memory, disk, and tx metrics
- validator table
- package/version snapshot
- historical charts from SQLite
- custom charts with multiple metrics
- custom chart config stored in JSON
- startup script with node IP/host input

## Stack

- backend: FastAPI
- frontend: React + Vite
- storage: SQLite
- custom graph config: JSON

## Project structure

```text
dashboard-monitor/
├── backend/
├── frontend/
└── start_install.sh
```

## Quick start

```bash
chmod +x start_install.sh
./start_install.sh
```

The script will ask for the node IP/host, configure the backend, initialize SQLite, and start:

***backend: http://127.0.0.1:8000***

***frontend: http://127.0.0.1:5173***

<img width="1880" height="932" alt="image" src="https://github.com/user-attachments/assets/8a9ce073-3a06-4939-84d4-80bf9750cdd7" />

<img width="1870" height="915" alt="image" src="https://github.com/user-attachments/assets/7dd61b5e-e320-4108-a482-f581d2d9f63c" />

<img width="1853" height="912" alt="image" src="https://github.com/user-attachments/assets/54ce03fb-1f7b-4524-91d8-743b4349a1b4" />
