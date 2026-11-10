"""Shared building blocks for every Twitter microservice.

Deliberately *thin*: only cross-cutting technical concerns live here
(events, resilience, observability, security). Business logic never goes
into a shared library -- that would re-couple the services (a classic
"distributed monolith" anti-pattern, see docs/02-microservice-patterns.md).
"""
