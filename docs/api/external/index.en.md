# External API Reference

External APIs are intended for library users and are safe to call from applications, scripts, and examples. These pages focus on stable usage rather than implementation details.

Recommended root imports:

```python
from astrum import (
    AstrumConfig,
    task,
    run,
    build_scheduler,
    SchedulerRegistry,
    DynamicScheduler,
    TaskOrder,
    Ref,
    F,
    T,
)
```

## Index

- [Configuration](config.md): `AstrumConfig`.
- [Decorators and Registry](decorators-registry.md): module-level `@task`, namespaces, and `SchedulerRegistry`.
- [Scheduler Execution](scheduler-execution.md): manual schedulers and `execute()`.
- [Data Transport Declarations](data-transport.md): `Ref` / `F` / `T` and explicit data objects.
- [Models and Errors](models-errors.md): execution reports, statistics, state enums, and user-facing errors.

