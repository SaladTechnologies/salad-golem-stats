# Golem x Salad stats integration proposal

# Overview

This document outlines the technical specification for integrating statistics from Salad’s private networks into the public Golem Stats dashboard at [https://stats.golem.network](https://stats.golem.network)

# Scope

This integration covers the **Golem Stats homepage only.** The homepage displays aggregate network statistics including total providers, resources, earnings and historical charts.  
If this initial integration is successful, we could proceed with a phase 2 integration that adds individual providers to our providers lists and details pages.

# Architecture

Our stats backend operates by running numerous asynchronous Celery tasks that continuously collect, aggregate, and cache data from the public Golem network. These tasks scan for online providers, compute resource totals, calculate pricing statistics, and track earnings—all stored in Redis for fast retrieval by our public API endpoints. Rather than attempting to replicate your provider entities in our database or building complex synchronization logic, the simplest integration approach is for you to expose a single endpoint with pre-aggregated statistics. We then pull this data into our Redis cache alongside our own aggregated stats, merging the numbers at query time. This keeps the integration lightweight, maintains clear data ownership on your side, and requires no schema changes or migrations on ours.

# Data requirements

The Golem Stats homepage displays data from five specific endpoints. This section describes exactly what data we need from your network to populate each component.

| Frontend Component | Data Type |
| :---- | :---- |
| Historical Network Stats Chart | Time-series by runtime |
| Network Earnings Overview | Aggregated earnings |
| Network Activity (Real-time) | Providers computing |
| Version Adoption Chart | Version distribution |
| Historical Computing Chart | Daily computing totals |

# API Specification

## 1\. Current Snapshot Endpoint

### Endpoint

`GET /v1/network/stats`

### Authentication

API token in the \`Authorization\` header:

`Authorization: Bearer <your-api-token>`

Please generate a shared secret and share it with us securely.

### Response schema

Complete response schema example:

```json
{
  "timestamp": "2025-12-15T10:30:00Z",
  "network_id": "salad",
  "providers": {
    "online": 150,
    "computing": 23
  },
  
  "resources": {
    "cores": 2400,
    "memory_gib": 9600.0,
    "disk_gib": 48000.0,
    "gpus": 12
  },
  
  "earnings": {
    "6h": 125.50,
    "24h": 450.25,
    "168h": 2800.00,
    "720h": 11500.00,
    "2160h": 32000.00,
    "total": 85000.00
  },
  
  "versions": [
    {"version": "0.16.0", "count": 80, "rc": false},
    {"version": "0.15.2", "count": 50, "rc": false},
    {"version": "0.17.0", "count": 20, "rc": true}
  ]
}
```

### Fields definitions

#### Root Fields

| Field | Type | Required | Description |
| :---- | :---- | :---- | :---- |
| timestamp | String (ISO 8601\) | Yes | When these statistics were collected |
| network\_id | String | Yes | Unique identifier for your network |

#### Providers Object

| Field | Type | Required | Description |
| :---- | :---- | :---- | :---- |
| online | Integer | Yes | Total providers currently online |
| computing | Integer | Yes | Providers actively computing tasks right now |

#### Resources Object

| Field | Type | Required | Description |
| :---- | :---- | :---- | :---- |
| cores | Integer | Yes | Total CPU cores across all online providers |
| memory\_gib | Float | Yes | Total memory in gibibytes (GiB) |
| disk\_gib | Float | Yes | Total disk storage in gibibytes (GiB) |
| gpus | Integer | Yes | Total GPU devices across all providers |

#### Earnings Object

All values are in GLM tokens.

| Field | Type | Required | Description |
| :---- | :---- | :---- | :---- |
| 6h | Float | Yes | Total GLM earned in the last 6 hours |
| 24h | Float | Yes | Total GLM earned in the last 24 hours |
| 168h | Float | Yes | Total GLM earned in the last 7 days (168 hours) |
| 720h | Float | Yes | Total GLM earned in the last 30 days (720 hours) |
| 2160h | Float | Yes | Total GLM earned in the last 90 days (2160 hours) |
| total | Float | Yes | Total GLM earned since network inception |

#### Versions Array

Array of objects describing the distribution of Yagna versions across your providers.

| Field | Type | Required | Description |
| :---- | :---- | :---- | :---- |
| version | String | Yes | Version string (e.g., "0.16.0"). Do not include "v" prefix. |
| count | Integer | Yes | Number of providers running this version |
| rc | Boolean | Yes | True if this is a release candidate or pre-release version |

## 2\. Historical Data Endpoint

### Endpoint

`GET /v1/network/stats/historical`

### Authentication

API token in the \`Authorization\` header:

`Authorization: Bearer <your-api-token>`

Please generate a shared secret and share it with us securely.

### Response schema

Complete response schema example:

```json
{
  "network_id": "salad",
  "network_stats": {
    "vm": [
      {
        "date": 1734220800,
        "online": 145,
        "cores": 2300,
        "memory_gib": 9200.0,
        "disk_gib": 46000.0,
        "gpus": 0
      },
      {
        "date": 1734307200,
        "online": 148,
        "cores": 2350,
        "memory_gib": 9400.0,
        "disk_gib": 47000.0,
        "gpus": 0
      }
    ],
    "vm-nvidia": [
      {
        "date": 1734220800,
        "online": 12,
        "cores": 192,
        "memory_gib": 768.0,
        "disk_gib": 3840.0,
        "gpus": 12
      }
    ]
  },
  "utilization": [
    [1734220800, 23],
    [1734220830, 25],
    [1734220860, 22]
  ],
  "computing_daily": [
    {"date": "2025-12-08", "total": 1250},
    {"date": "2025-12-09", "total": 1340},
    {"date": "2025-12-10", "total": 1180}
  ]
}
```

### Field Definitions

#### network\_stats Object

A map of runtime names to arrays of historical data points.  
Each data point contains:

| Field | Type | Required | Description |
| :---- | :---- | :---- | :---- |
| date | Integer | Yes | Unix timestamp in seconds |
| online | Integer | Yes | Number of providers online at this time |
| cores | Integer | Yes | Total CPU cores at this time |
| memory\_gib | Float | Yes | Total memory in GiB at this time |
| disk\_gib | Float | Yes | Total disk in GiB at this time |
| gpus | Integer | Yes | Total GPUs at this time |

**Data granularity**: Hourly data points for the last 24 hours, daily data points for older data. Provide at least 30 days of history if available.

#### Utilization Array

An array of data points showing providers actively computing over time. Each element is a two-element array:

| Index | Type | Description |
| :---- | :---- | :---- |
| 0 | Integer | Unix timestamp in seconds |
| 1 | Integer | Number of providers computing at this moment |

**Data granularity**: 30-second intervals for the last 6 hours.

#### computing\_daily Array

Daily aggregates of computing activity for historical charts.

| Field | Type | Required | Description |
| :---- | :---- | :---- | :---- |
| date | String | Yes | Date in YYYY-MM-DD format |
| total | Integer | Yes | Sum of maximum concurrent computing providers for that day |

**Data range**: Provide as much history as available, ideally at least 90 days.

# Monitoring

Once integrated, you can verify the integration status at:

`GET https://api.stats.golem.network/v2/network/partner/status`

Example response:

```json
{
  "enabled": true,
  "cached": true,
  "network_id": "salad",
  "timestamp": "2025-12-15T10:30:00Z",
  "providers_online": 150,
  "total_cores": 2400,
  "total_memory_gib": 9600.0
}
```

