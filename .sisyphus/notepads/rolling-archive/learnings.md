# Learnings
- Implementing an abstract RollingArchiveBackend interface helps decouple core archive logic from backend implementations.
- The interface includes enumerating groups, retrieving per-group timestamps, deleting groups (with dry-run support), and reporting backend type.
- Google-style docstrings and explicit type hints improve usability and static checks for implementers.
- Do not include any concrete logic or backend-specific imports in the abstract base class.
