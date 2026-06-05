# Decorators and Registry

The decorator API is the recommended way to use Astrum. It registers functions as DAG tasks, then converts them into the task callables and `TaskOrder` objects required by `DynamicScheduler`.

## Module-level entry points

::: astrum.decorators.task

::: astrum.decorators.run

::: astrum.decorators.build_scheduler

::: astrum.decorators.build_task_orders

## Namespaces

::: astrum.decorators.use_namespace

::: astrum.decorators.get_registry

::: astrum.decorators.clear_registry

::: astrum.decorators.active_namespace

## SchedulerRegistry

`SchedulerRegistry` is useful when you want an explicit workflow object instead of relying on global namespaces.

::: astrum.decorators.SchedulerRegistry
    options:
      members:
        - task
        - get_task
        - get_all_tasks
        - clear
        - build_task_orders
        - build_tasks
        - build_scheduler
        - run

::: astrum.decorators.RegisteredTask

