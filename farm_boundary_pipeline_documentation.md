# Farm Boundary + ET Intersection Pipeline — Technical Documentation

**Project**: CoRE Stack — NRM Module  
**Author**: Akshat Kumar  
**Date**: June 2026  
**Status**: Tested on Bhinmal tehsil (Jalor, Rajasthan) — awaiting review before full production run

---

## 1. Pipeline Overview

The pipeline processes a single **tehsil** (block-level administrative unit) in three sequential phases:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│   INPUT: State, District, Block (Tehsil), Year                              │
│                                                                             │
│   Phase 1 ──► Phase 2 ──► Phase 3                                           │
│   (Fetch)     (Clean)     (ET Intersect)                                    │
│                                                                             │
│   OUTPUT: farm_boundaries_et.parquet                                        │
│           (one row per farm, with geometry + monthly AET + stress indicator) │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

The pipeline is triggered via a REST API call and executed asynchronously by a Celery worker.

---

## 2. Phase 1 — Fetch Raw Farm Boundaries

**What it does**: Queries the Google AnthroKrishi API to retrieve farm boundary polygons for the given tehsil.

**How it works**:
1. Loads the official tehsil polygon from the Survey of India (SOI) shapefile.
2. Computes the bounding box of the tehsil.
3. Divides the bounding box into a grid of **S2 Level-13 cells** (~600–1000 cells per tehsil).
4. For each cell, calls the AnthroKrishi API to fetch all farm outlines detected within that cell.
5. Saves each API response as a JSON file in `data/farm_boundaries/{state}/{district}/{block}/raw/`.
6. Maintains a crash-safe `manifest.json` so interrupted runs can resume from where they left off.

**Output**: Raw JSON files (one per S2 cell) stored on disk.

| Property | Value |
|---|---|
| Output directory | `data/farm_boundaries/{state}/{district}/{block}/raw/` |
| File format | JSON (one file per S2 cell) |
| Resumable | Yes — skips cells already fetched |
| Typical time | 1–2 minutes (network-bound) |

---

## 3. Phase 2 — Clean, Clip, and Convert to GeoParquet

**What it does**: Reads all raw JSON files, extracts farm polygons, filters to only agricultural field boundaries, clips them to the exact tehsil boundary, and saves as a single GeoParquet file.

**How it works**:
1. Reads all raw JSON files from Phase 1.
2. Parses the embedded GeoJSON geometries and extracts features with `alu_type = "FARM_FIELD"`.
3. Builds a GeoDataFrame with Shapely geometries.
4. Clips all polygons to the official SOI tehsil boundary (removes farms outside the tehsil).
5. Deduplicates by `farm_uid`.
6. Saves as GeoParquet (compressed, fast-loading columnar format).

**Output**: `farm_boundaries.parquet`

### Output Schema — Phase 2

| # | Column | Type | Description |
|---|---|---|---|
| 1 | `farm_id` | string | Unique identifier: `{state}_{district}_{block}_{index}` |
| 2 | `farm_uid` | string | AnthroKrishi unique ID for the farm |
| 3 | `cell_token` | string | S2 cell token where this farm was fetched |
| 4 | `alu_type` | string | Agricultural land use type (always `FARM_FIELD` after filtering) |
| 5 | `plus_code` | string | Google Plus Code for the farm's location |
| 6 | `area_m2` | float | Farm area in square meters |
| 7 | `class_confidence` | float | ML classification confidence (0–1) |
| 8 | `capture_date` | string | Date when satellite imagery was captured |
| 9 | `geometry` | geometry | Farm boundary polygon (EPSG:4326, WGS 84) |

**Total columns**: 9

---

## 4. Phase 3 — ET Intersection (AET Raster × Farm Polygons)

**What it does**: Downloads the AET (Actual Evapotranspiration) raster from Google Earth Engine, intersects it with every farm polygon using **vectorised rasterisation**, and appends monthly AET values + a water stress indicator.

### 4.1 Data Source

| Property | Value |
|---|---|
| GEE Asset Path | `projects/corestack-datasets-alpha/assets/datasets/et_downscale/aet_aez_{zone}_{year}` |
| Resolution | 30 meters per pixel |
| Bands | 13 (b1–b12 = monthly AET in mm/day, b13 = annual summary) |
| Coverage | AEZ 2 (Hot Arid Zone — western Rajasthan, Gujarat) |
| Years available | 2017–2024 |

### 4.2 Spatial Intersection Method — Vectorised Rasterisation

Instead of sampling a single centroid pixel per farm, the pipeline uses the **vectorised rasterisation** approach:

```
Step 1: RASTERISE all farm polygons into a labelled grid
        (same 30m resolution as the AET image)

        ┌─────┬─────┬─────┬─────┬─────┐
        │  0  │  0  │  0  │  0  │  0  │   0 = background (no farm)
        ├─────┼─────┼─────┼─────┼─────┤
        │  0  │ 42  │ 42  │ 42  │  0  │   42 = Farm #42
        ├─────┼─────┼─────┼─────┼─────┤
        │  0  │ 42  │ 42  │ 43  │ 43  │   43 = Farm #43
        ├─────┼─────┼─────┼─────┼─────┤
        │  0  │  0  │ 43  │ 43  │  0  │
        └─────┴─────┴─────┴─────┴─────┘

Step 2: READ the AET band as a NumPy array

        ┌─────┬─────┬─────┬─────┬─────┐
        │ 0.1 │ 0.3 │ 0.4 │ 0.2 │ 0.1 │
        ├─────┼─────┼─────┼─────┼─────┤
        │ 0.2 │ 0.5 │ 0.7 │ 0.6 │ 0.3 │
        ├─────┼─────┼─────┼─────┼─────┤
        │ 0.1 │ 0.4 │ 0.8 │ 1.2 │ 0.9 │
        ├─────┼─────┼─────┼─────┼─────┤
        │ 0.2 │ 0.3 │ 1.1 │ 0.7 │ 0.4 │
        └─────┴─────┴─────┴─────┴─────┘

Step 3: COMPUTE mean AET per farm label using np.bincount

        Farm #42: pixels = [0.5, 0.7, 0.6, 0.4, 0.8]  → mean = 0.60
        Farm #43: pixels = [1.2, 0.9, 1.1, 0.7]        → mean = 0.975

Step 4: REPEAT for all 13 bands (Jan–Dec + Annual)
```

**Why this approach?**
- **Accurate**: Averages ALL pixels within each farm, not just the centre point.
- **Fast**: All 50K farms are processed in a single NumPy pass (~2 seconds per band).
- **No double-counting**: Each pixel is assigned to exactly one farm.

### 4.3 AET Raster Download

The 13-band AET image is downloaded from GEE **band by band** (each ~5–7 MB) to stay within the 50 MB per-request GEE download limit, then merged locally into a single multi-band GeoTIFF using `rasterio`.

### 4.4 Water Stress Indicator

For each farm, we count the number of months where AET falls below a threshold:

```
water_stress_months = count of months where AET < 0.5 mm/day
```

> **Note**: The threshold of 0.5 mm/day is a placeholder. Once PET (Potential Evapotranspiration) data is available from Aman, this will be replaced with the standard **AET/PET ratio** metric.

### 4.5 Output Schema — Phase 3

**Output file**: `farm_boundaries_et.parquet`

| # | Column | Type | Source | Description |
|---|---|---|---|---|
| 1 | `farm_id` | string | Phase 2 | Unique farm identifier |
| 2 | `farm_uid` | string | Phase 2 | AnthroKrishi unique ID |
| 3 | `cell_token` | string | Phase 2 | S2 cell token |
| 4 | `alu_type` | string | Phase 2 | Land use type |
| 5 | `plus_code` | string | Phase 2 | Google Plus Code |
| 6 | `area_m2` | float | Phase 2 | Farm area (m²) |
| 7 | `class_confidence` | float | Phase 2 | ML classification confidence |
| 8 | `capture_date` | string | Phase 2 | Satellite capture date |
| 9 | `geometry` | geometry | Phase 2 | Farm boundary polygon |
| 10 | `aet_jan` | float | **Phase 3** | Mean AET for January (mm/day) |
| 11 | `aet_feb` | float | **Phase 3** | Mean AET for February (mm/day) |
| 12 | `aet_mar` | float | **Phase 3** | Mean AET for March (mm/day) |
| 13 | `aet_apr` | float | **Phase 3** | Mean AET for April (mm/day) |
| 14 | `aet_may` | float | **Phase 3** | Mean AET for May (mm/day) |
| 15 | `aet_jun` | float | **Phase 3** | Mean AET for June (mm/day) |
| 16 | `aet_jul` | float | **Phase 3** | Mean AET for July (mm/day) |
| 17 | `aet_aug` | float | **Phase 3** | Mean AET for August (mm/day) |
| 18 | `aet_sep` | float | **Phase 3** | Mean AET for September (mm/day) |
| 19 | `aet_oct` | float | **Phase 3** | Mean AET for October (mm/day) |
| 20 | `aet_nov` | float | **Phase 3** | Mean AET for November (mm/day) |
| 21 | `aet_dec` | float | **Phase 3** | Mean AET for December (mm/day) |
| 22 | `aet_annual` | float | **Phase 3** | Mean annual AET (mm) |
| 23 | `water_stress_months` | int | **Phase 3** | Count of months with AET < threshold |

**Total columns**: 23 (9 from Phase 2 + 14 from Phase 3)

---

## 5. Validation Results

Tested on: **Bhinmal, Jalor (Rajasthan) — Year 2017**

| Metric | Value |
|---|---|
| Total farms processed | 50,311 |
| Farms with valid AET data | 46,359 (92.1%) |
| Farms without data | 3,952 (too small to cover one 30m pixel) |
| Average annual AET | 183.6 mm |
| Average water stress months | 7.2 out of 12 |
| Phase 3 processing time | ~8 seconds |

### Pixel-Level Verification

For an individual farm (ID: `rajasthan_jalor_bhinmal_043817`, area: 0.89 ha):

| Method | Pixels | Mean AET (Jan) |
|---|---|---|
| Pipeline (vectorised rasterise) | 10 | 0.4016 |
| Manual single-farm clip (`rasterio.mask`) | 20 | 0.4602 |

The 10-pixel difference is because **neighbouring farms share edge pixels** — in the rasterised grid, each pixel is assigned to exactly one farm (no double-counting), whereas `rasterio.mask` clips for a single farm in isolation. The pipeline approach is the **correct** methodology for wall-to-wall farm analysis since it prevents the same pixel from being counted twice across adjacent farms.

---

## 6. Open Questions for Review

Before running the pipeline across all tehsils, the following decisions are needed:

### 6.1 Multi-Year Data Structure
Currently, each run produces `farm_boundaries_et.parquet` for one year. For multiple years (2017–2024), should we:
- **Option A**: Generate separate files per year (`et_2017.parquet`, `et_2018.parquet`) linked by `farm_uid`?
- **Option B**: Combine into a single "long format" table with `year` and `month` as rows instead of columns?

### 6.2 PET Data Integration
Aman is preparing the PET (Potential Evapotranspiration) dataset. Once available:
- Should we add 13 more columns (`pet_jan`...`pet_dec`, `pet_annual`) alongside AET columns?
- Or compute the `AET/PET` ratio directly and store the ratio?
- What AET/PET threshold defines "water stressed"?

### 6.3 AEZ Zone Coverage
The current AET data covers only **AEZ 2 (Hot Arid)**. Tehsils in other zones (e.g., Jaipur/Sanganer) have no AET coverage. Should the pipeline:
- Skip tehsils outside AEZ 2 for now?
- Wait until Aman provides assets for other AEZ zones?

### 6.4 Small Farm Handling
~8% of farms are smaller than one 30m pixel and receive no AET value (`NaN`). Options:
- Leave as `NaN` (current behaviour).
- Fall back to centroid sampling for these farms only.
- Exclude them from analysis entirely.

---

## 7. File Structure on Disk

```
data/farm_boundaries/
└── rajasthan/
    └── jalor/
        └── bhinmal/
            ├── raw/                              ← Phase 1 output (JSON files)
            │   ├── 39436a44.json
            │   ├── 39436a4c.json
            │   └── ... (937 files)
            ├── manifest.json                     ← Phase 1 resume tracking
            ├── farm_boundaries.parquet           ← Phase 2 output (9 columns)
            ├── aet_2017.tif                      ← Phase 3 downloaded raster (cached)
            └── farm_boundaries_et.parquet        ← Phase 3 output (23 columns)
```

---

## 8. Technology Stack

| Component | Technology |
|---|---|
| Task queue | Celery (Redis broker) |
| Farm boundary API | Google AnthroKrishi |
| ET data source | Google Earth Engine |
| Spatial operations | GeoPandas, Rasterio, Shapely |
| Vectorised rasterisation | `rasterio.features.rasterize()` + `numpy.bincount()` |
| Data format | GeoParquet (Apache Parquet with geometry) |
| Admin boundaries | Survey of India (SOI) tehsil shapefile |
