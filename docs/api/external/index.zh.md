# 外部 API Reference

外部 API 是面向库使用者的入口，适合在应用代码、脚本和示例中直接调用。这里优先记录稳定使用方式，而不是把所有实现细节都暴露给用户。

推荐从根包导入常用对象：

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

## 页面索引

- [配置](config.md)：`AstrumConfig`。
- [装饰器与注册表](decorators-registry.md)：模块级 `@task`、命名空间、`SchedulerRegistry`。
- [调度执行](scheduler-execution.md)：手动 scheduler 和 `execute()`。
- [数据传输声明](data-transport.md)：`Ref` / `F` / `T` 与显式数据对象。
- [模型与异常](models-errors.md)：执行报告、统计对象、状态枚举和用户可处理异常。

