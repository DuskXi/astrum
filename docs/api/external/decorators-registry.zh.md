# 装饰器与注册表

装饰器 API 是 Astrum 最推荐的使用方式。它把函数注册为 DAG task，并在构建 scheduler 时转换成底层 `DynamicScheduler` 所需的任务函数和 `TaskOrder`。

## 模块级入口

::: astrum.decorators.task

::: astrum.decorators.run

::: astrum.decorators.build_scheduler

::: astrum.decorators.build_task_orders

## 命名空间

::: astrum.decorators.use_namespace

::: astrum.decorators.get_registry

::: astrum.decorators.clear_registry

::: astrum.decorators.active_namespace

## SchedulerRegistry

`SchedulerRegistry` 适合显式创建独立工作流对象，避免依赖全局命名空间。

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

