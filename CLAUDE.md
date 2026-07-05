# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Extensions to [RDFSharp](https://github.com/mdesalvo/RDFSharp) that implement `RDFStore` to persist and query RDF data on external backends. Each `RDFSharp.Extensions.<Provider>/` folder is an independent NuGet project for a single provider: AzureTable, Firebird, MySQL, Neo4j, Oracle, PostgreSQL, SQLite, SQLServer.

## Build

```bash
dotnet build RDFSharp.Extensions.sln                          # all projects
dotnet build RDFSharp.Extensions.SQLite/RDFSharp.Extensions.SQLite.csproj   # single provider
```

Targets: `netstandard2.0` and `net8.0` (multi-targeting). Conditional code `#if NET8_0_OR_GREATER` enables `IAsyncDisposable` and native async APIs (`ExecuteReaderAsync`, `Connection.CloseAsync`, etc.) only on net8.0; netstandard2.0 falls back to the sync counterparts.

There are no test projects (no test `.csproj` in the solution). Each provider ships a `*Tests.txt` file (e.g. `SQLiteTests.txt`) described as the "official test suite": it's a top-level C# script (xUnit `Assert` calls + direct store operations) meant to be pasted into a manual runner (requires a real backend instance: local SQLite DB, Azurite, MySQL/PostgreSQL/Oracle/Firebird/SQL Server server, Neo4j instance). It is not part of the automated build/CI pipeline.

## Architecture shared across providers

Every provider follows the same 3-file pattern (except Neo4j and AzureTable, which are simpler since they're non-relational/manager-less):

- **`RDF<Provider>Store.cs`** — `sealed` class inheriting `RDFStore` (from RDFSharp) implementing `IDisposable` (+ `IAsyncDisposable` on net8.0). Holds only the connection string (and, for relational stores, an `Options` instance) as instance fields — no shared `Connection`/`Command` fields. Every operation opens its own local connection and command, scoped to that single call, relying on the ADO.NET provider's own connection pooling; this makes store instances safe to call concurrently (e.g. as a singleton in an ASP.NET Core DI container). Implements all abstract `RDFStore` members: `AddQuadruple`, `RemoveQuadruple(s)`, `RemoveQuadruplesBy*`, `MergeGraph`, `SelectQuadruples`, `ContainsQuadruple`, `QuadruplesCount`. Every method has a sync/async pair, where the sync version is almost always a thin wrapper (`XxxAsync().GetAwaiter().GetResult()`).
- **`RDF<Provider>StoreManager.cs`** (relational stores) — manages the connection and idempotent schema initialization (`CREATE TABLE IF NOT EXISTS Quadruples`, indexes on `ContextID`, `SubjectID`, `PredicateID`, `ObjectID`+`TripleFlavor` and combinations to optimize CSPOL patterns).
- **`RDF<Provider>StoreOptions.cs`** — configurable timeouts for Select/Insert/Delete, optionally passed to the store's constructor.

`Quadruples` table (schema shared across relational backends): columns `QuadrupleID` (unique hash, PK), `TripleFlavor` (SPO vs SPL), `Context`/`ContextID`, `Subject`/`SubjectID`, `Predicate`/`PredicateID`, `Object`/`ObjectID` — the `*ID` columns are hashes used for indexing/lookup, the plain columns hold the original RDF values (parsed via `RDFStoreUtilities.ParseQuadruple`).

Select/delete queries for C-S-P-O-L combinations (CSPOL accessors: Context, Subject, Predicate, Object, Literal — `null` = wildcard, Object and Literal are mutually exclusive) are built dynamically (query builder, no longer switch-statements after the refactor in `ae9b551`) in private methods like `PrepareSelectDeleteCommand` that compose the `WHERE` clause and parameters based on which accessors are set.

Connection handling pattern: create a local connection from the stored connection string → open → execute command → close + dispose, on both the success and exception paths (never leave connections open on error); exceptions are always caught and rethrown as `RDFStoreException` with a contextual message. `ToString()` (used to compute `StoreID`) derives server/database info from a provider-specific connection-string builder (e.g. `SQLiteConnectionStringBuilder`) rather than from an open connection.

AzureTable's OData filters (`BuildQuadrupleFilter`) are built as a LINQ `Expression<Func<RDFAzureTableQuadruple,bool>>` (translated safely by the Azure SDK), not by interpolating RDF term values into a filter string — RDF terms can contain unescaped quotes.

Neo4j (`RDFNeo4jStore.cs`) and AzureTable (`RDFAzureTableStore.cs`/`RDFAzureTableQuadruple.cs`) don't follow the relational schema: Neo4j uses `Neo4j.Driver` with direct Cypher queries against the store (no manager/SQL schema), AzureTable models the quadruple as a table entity (`RDFAzureTableQuadruple`) with partition/row key.

## Conventions to follow when modifying a store

- Keep the sync/async pairing for every public operation; if you add a method, add both forms.
- Object and Literal remain mutually exclusive wherever they appear as CSPOL parameters — validate with a guard and `RDFStoreException` like the other methods already do.
- Never store a `Connection`/`Command` as a shared instance field on a relational store — always open a local connection (and local command) inside the method, and close/dispose it in a `finally` (or on both success/exception paths). This is what keeps store instances thread-safe under concurrent calls.
- Never build a query filter (SQL, OData, Cypher) by interpolating an RDF term's `ToString()` value into the query/filter text — RDF terms can contain unescaped quotes. Use parameterized queries (relational stores) or the provider's expression/filter-builder API (e.g. AzureTable's `Expression<Func<T,bool>>` overload).
- The constructor initializes `StoreType`, `ConnectionString` (and `Options`), and `StoreID` (hash of `ToString()`) — no connection is opened or kept alive by the constructor beyond the one-time schema initialization done via the `StoreManager`.
