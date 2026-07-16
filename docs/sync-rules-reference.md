# sync-rules.toml reference

`sync-rules.toml` defines the buckets your sync gateway uses to partition
data per authenticated user (or tenant, or any other JWT-derived scope). It
is parsed by [`pes_rules::parse_sync_rules`] into a `SyncRuleSet`, then
checked by [`pes_rules::validate`] before the gateway will load it.

## Top-level shape

```toml
version = "1"

[buckets.<bucket_id>]
description = "..."          # optional
parameters = ["param_a", ...]

[buckets.<bucket_id>.parameter_queries]
param_a = "SELECT ... WHERE auth_user_id = $1"

[buckets.<bucket_id>.data]
query_name = "SELECT ... WHERE col = {bucket_parameters.param_a}"
```

- `version` is optional and defaults to `"1"`.
- Each `[buckets.<bucket_id>]` table declares one bucket. The bucket id is
  the TOML table key itself, not a separate `id` field.
- `parameters` lists the names this bucket needs resolved from the
  authenticated user's JWT claims before any data query can run.
- `parameter_queries` maps each parameter name to a SQL query that resolves
  it. The query receives the JWT `sub` claim as its only bind parameter,
  `$1`.
- `data` maps arbitrary query names to the SQL that selects the rows
  belonging to this bucket. Data queries may reference resolved parameter
  values via `{bucket_parameters.<name>}` template substitution.

## Full example

```toml
version = "1"

[buckets.user_entities]
description = "All entities owned by the authenticated user"
parameters = ["user_id"]

[buckets.user_entities.parameter_queries]
user_id = "SELECT id FROM users WHERE auth_user_id = $1"

[buckets.user_entities.data]
entities = "SELECT * FROM entities WHERE owner_id = {bucket_parameters.user_id}"
tags = "SELECT * FROM entity_tags WHERE entity_id IN (SELECT id FROM entities WHERE owner_id = {bucket_parameters.user_id})"

[buckets.tenant_shared]
description = "Reference data shared across a tenant"
parameters = ["tenant_id"]

[buckets.tenant_shared.parameter_queries]
tenant_id = "SELECT tenant_id FROM users WHERE auth_user_id = $1"

[buckets.tenant_shared.data]
entity_types = "SELECT * FROM entity_types WHERE tenant_id = {bucket_parameters.tenant_id}"
```

## Validation rules

A parsed `sync-rules.toml` is rejected if any of the following hold:

1. **Missing parameter query.** Every name listed in `parameters` must have
   a corresponding entry in `parameter_queries`.
2. **Wrong placeholder.** Every `parameter_queries` value must contain the
   placeholder `$1` and no other numbered placeholder (`$2`, `$10`, etc.).
   `$1` may appear more than once in the same query.
3. **Undeclared bucket parameter reference.** Every
   `{bucket_parameters.X}` reference inside a `data` query must name a
   parameter that was declared in this bucket's `parameters` list.
4. **Invalid bucket id.** Bucket ids (the TOML table keys under
   `[buckets.*]`) must match `[a-z][a-z0-9_-]*` — lowercase ASCII letters,
   digits, underscores, and hyphens, starting with a letter.

Circular bucket references are not currently representable in the DSL: a
data query may only reference its own bucket's resolved parameters via
`{bucket_parameters.X}`, never another bucket's data or parameters. There is
therefore nothing for a cycle-detection pass to find yet — this is a design
constraint of the DSL, not an unimplemented check.

## Security note

`parameter_queries` values are executed as parameterized SQL with the JWT
`sub` claim bound to `$1` — never string-interpolated. `{bucket_parameters.X}`
substitution in `data` queries only ever substitutes *already-validated,
already-resolved* parameter values (the output of a `parameter_queries`
run), not raw user input. This is what makes the bucket assignment boundary
safe against SQL injection from a forged or crafted JWT claim; see the
`pes-rules` bucket assigner documentation for the full security model.

## Fixture files

See `crates/pes-rules/tests/fixtures/valid/` for parseable examples covering
multi-bucket documents, zero-parameter buckets, multi-parameter buckets, and
repeated placeholder use. See `crates/pes-rules/tests/fixtures/invalid/` for
one example per rejected error class (TOML syntax errors, each of the four
validation rules above).
