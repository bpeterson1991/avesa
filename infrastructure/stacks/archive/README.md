# Archived Infrastructure Stacks

This directory contains infrastructure stacks that have been archived as part of the Phase 4 infrastructure simplification.

## Archived Stacks

### CrossAccountMonitoringStack
- **File**: `cross_account_monitoring.py`
- **Reason for Archiving**: This stack is designed for future multi-account deployments and cross-account monitoring capabilities. It's not currently needed for the single-account deployment model.
- **Future Use**: When AVESA expands to a multi-account architecture (e.g., separate production and non-production accounts), this stack can be restored and deployed.
- **Dependencies**: None - this was a standalone monitoring stack.

## Restoration Process

To restore any archived stack:

1. Move the stack file back to `infrastructure/stacks/`
2. Add the import back to `infrastructure/app.py`
3. Instantiate the stack in the app.py deployment section
4. Update `scripts/deploy.sh` to include the stack in deployment

## Safety Notes

- These stacks were archived (not deleted) to preserve the implementation for future use
- All archived stacks have been verified to have no active dependencies
- The archive preserves the complete implementation and documentation