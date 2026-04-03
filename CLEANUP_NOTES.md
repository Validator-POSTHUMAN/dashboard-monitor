# Cleanup and frontend build fixes

## Removed from archive
- `backend/.env`
- `backend/__pycache__/`
- `backend/package-lock.json`
- `backend/history/history.db`
- `backend/history/cat`
- `frontend/node_modules/`
- `frontend/dist/`
- `frontend/tsconfig.tsbuildinfo`
- `frontend/src/components/GenLayerValidatorDashboard.tsx.backup`

## Added
- `.gitignore`
- `frontend/vite-env.d.ts`

## Changed
- `frontend/tsconfig.json`
  - switched `moduleResolution` from `Node` to `Bundler`
  - included `vite-env.d.ts`
- `frontend/src/components/GenLayerValidatorDashboard.tsx`
  - fixed TypeScript narrowing for mixed point shapes
  - fixed uPlot options typing
  - fixed aligned data typing
  - changed chart alignment to use a merged X axis across all series
  - fixed empty-state detection for multi-series charts
  - simplified history fallback assignment
- `frontend/src/index.css`
  - added selection styling for uPlot drag-zoom box
- `backend/main.py`
  - aligned `summary.overallHealth` with the status used to infer local validator health in SQLite fallback mode

## Verification
- Python syntax checked with `python3 -m py_compile`
- Frontend build checked with `npm run build`
