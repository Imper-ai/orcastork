# orcastork (TypeScript)

The TypeScript port of [orcastork](../README.md), **in progress**. The Python packages in
`../orcastork` and `../orcastork_lite` are the specification: same behaviour, same invariants,
same wire formats. Not usable yet — the foundations are landing first.

```sh
make deps    # npm ci
make lint    # biome check + tsc --noEmit
make test    # vitest run (spawns the servers it needs)
make build   # tsc -> dist/
```

[CLAUDE.md](CLAUDE.md) is the conventions contract: layout, the Python → TypeScript idiom table,
and the rules a change here has to keep. Read it before writing code. The full README — what the
framework is and how to build on it — arrives with the public surface it documents.
