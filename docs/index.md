# realtime

The `realtime` provider plugin for [togo](https://github.com/togo-framework/togo).

## Install

```bash
togo install togo-framework/realtime
```

On import it self-registers with the kernel (priority-ordered provider). Access it
via the app container in your handlers/actions (e.g. `a.REALTIME`). Swap the default by
registering another provider for the same capability.
