import inspect
import ast
import types
import textwrap
import warnings
from typing import Optional, Union, get_origin, get_args, Any, Callable

from pydantic import BaseModel

# 定义允许的数据模型别名
AllowDataModelType = Optional[Union[type[BaseModel], type[dict], type[list]]]


class TypeMatchError(TypeError):
    """自定义类型匹配错误

    Custom type matching error.
    """

    pass


# ==========================================
# 1. 核心工具函数：获取真实函数对象与基础类型对比
# ==========================================


def _get_real_function(func_obj: Any) -> Callable:
    """
    最大兼容性：从普通函数、异步函数或未执行的协程对象中提取真实的函数签名引用
    """
    if inspect.isfunction(func_obj) or inspect.ismethod(func_obj) or inspect.isbuiltin(func_obj):
        return func_obj

    # 如果传入的是协程对象 (coroutine object) 如: coro = async_func()
    if isinstance(func_obj, types.CoroutineType):
        name = func_obj.__name__
        frame = func_obj.cr_frame
        # 尝试通过协程帧的全局变量找回原函数
        if frame and name in frame.f_globals:
            return frame.f_globals[name]
        raise ValueError("无法从该协程对象中提取函数签名，可能由于其不在全局作用域或帧已释放。")

    if callable(func_obj):
        return func_obj.__call__

    raise TypeError(f"不支持的函数对象类型: {type(func_obj)}")


def _is_type_compatible(annotated_type: Any, allow_data_model: AllowDataModelType) -> bool:
    """
    判断类型是否兼容，支持泛型提取 (如 Optional[dict], Union[BaseModel, dict])
    """
    if allow_data_model is None:
        return True  # 如果未指定约束，默认通过

    origin = get_origin(annotated_type)

    # 兼容 Union (比如 Optional[T] 实际上是 Union[T, NoneType])
    if origin is Union or getattr(types, "UnionType", None) and origin is types.UnionType:
        # 只要 Union 中有一个类型满足条件即算匹配
        return any(_is_type_compatible(arg, allow_data_model) for arg in get_args(annotated_type) if arg is not type(None))

    # 归一化类型 (剥离泛型外壳，如把 dict[str, Any] 变成 dict)
    actual_type = origin if origin is not None else annotated_type

    try:
        if allow_data_model is dict:
            return issubclass(actual_type, dict)
        elif allow_data_model is list:
            return issubclass(actual_type, list)
        elif isinstance(allow_data_model, type) and issubclass(allow_data_model, BaseModel):
            return issubclass(actual_type, allow_data_model)
    except TypeError:
        # 对于一些无法被 issubclass 判断的特殊 Typing 类型，进行容错捕获
        return False

    return False


# ==========================================
# 2. 输入匹配函数 (Input Matcher)
# ==========================================


def match_input_type(func_obj: Any, allow_data_model: AllowDataModelType, key: Optional[str] = None, index: Optional[int] = None, single_object_mode: bool = False) -> None:
    """
    匹配输入参数的类型。

    Match the type of input parameters.
    """
    real_func = _get_real_function(func_obj)
    sig = inspect.signature(real_func)
    params = list(sig.parameters.values())

    target_param = None

    # 路由逻辑：单对象模式 vs Key/Index 定位
    if single_object_mode:
        if not params:
            raise ValueError(f"启用单对象模式失败: 函数 {real_func.__name__} 没有输入参数")
        target_param = params[0]  # 单对象模式忽略 key 和 index，直接取第一个
    else:
        if key is None and index is None:
            raise ValueError("非单对象模式下，必须提供 key 或 index。如果不提供，请将 single_object_mode 设为 True")

        if key is not None:  # Key 优先级最高，有 Key 忽略 Index
            if key in sig.parameters:
                target_param = sig.parameters[key]
            else:
                raise ValueError(f"函数 {real_func.__name__} 中未找到参数名: {key}")
        elif index is not None:
            if 0 <= index < len(params):
                target_param = params[index]
            else:
                raise IndexError(f"索引 {index} 超出函数参数范围 (共 {len(params)} 个参数)")

    # 检查注解
    annotated_type = target_param.annotation
    if annotated_type == inspect.Parameter.empty:
        warnings.warn(f"警告：参数 '{target_param.name}' 没有定义类型注解，跳过严格类型检查。")
        return

    if not _is_type_compatible(annotated_type, allow_data_model):
        raise TypeMatchError(f"输入类型不匹配！参数 '{target_param.name}' 类型为 {annotated_type}, " f"但要求的 allow_data_model 为 {allow_data_model}")


# ==========================================
# 3. 输出匹配函数与 AST 推断引擎 (Output Matcher)
# ==========================================
def match_output_type(func_obj: Any, allow_data_model: AllowDataModelType, max_depth: int = 5) -> None:
    """
    匹配输出值的类型。
    融合版：支持标准注解解析 + 高级跨函数 AST 静态类型推断。

    Match the type of output values.
    Merged version: supports standard annotation parsing plus advanced
    cross-function AST static type inference.
    """
    real_func = _get_real_function(func_obj)
    sig = inspect.signature(real_func)

    # ==========================================
    # 第一阶段：优先使用标准的返回类型注解
    # ==========================================
    if sig.return_annotation != inspect.Signature.empty:
        if _is_type_compatible(sig.return_annotation, allow_data_model):
            return
        raise TypeMatchError(f"输出类型不匹配！函数声明的返回类型为 {sig.return_annotation}, " f"而要求的 allow_data_model 为 {allow_data_model}")

    # ==========================================
    # 第二阶段：启动高级 AST 推断引擎 (跨函数 & 运算推断)
    # ==========================================
    warnings.warn(f"函数 {real_func.__name__} 没有定义返回类型注解，正在启动高级 AST 跨函数类型推断...")

    # 提取全局变量空间，用于突破单函数的墙，寻找外部函数
    global_vars = real_func.__globals__ if hasattr(real_func, "__globals__") else {}

    def parse_and_infer(func_to_parse: Callable, current_depth: int) -> set:
        if current_depth > max_depth:
            return {Any}

        try:
            source = textwrap.dedent(inspect.getsource(func_to_parse))
            tree = ast.parse(source)
        except Exception:
            return {Any}

        # 1. 收集当前函数的局部变量
        context_vars = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        context_vars[target.id] = node.value

        # 2. 节点推断逻辑
        def infer_node(node: ast.AST, depth: int) -> Any:
            if depth > max_depth:
                return Any

            # 数学运算
            if isinstance(node, ast.BinOp):
                return int
            # 基础结构
            if isinstance(node, ast.Dict):
                return dict
            if isinstance(node, (ast.List, ast.ListComp)):
                return list
            if isinstance(node, ast.Constant):
                return type(node.value)

            # 跨函数或类实例化识别
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                func_name = node.func.id
                # 如果调用的是同环境下的另一个函数，则深度跳入解析
                if func_name in global_vars and callable(global_vars[func_name]):
                    sub_types = parse_and_infer(global_vars[func_name], depth + 1)
                    return list(sub_types)[0] if sub_types else Any
                return func_name  # 当作 BaseModel 类名处理

            # 局部变量溯源
            if isinstance(node, ast.Name) and node.id in context_vars:
                return infer_node(context_vars[node.id], depth + 1)

            return Any

        # 3. 提取所有 Return 的结果
        inferred = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Return) and node.value:
                inferred.add(infer_node(node.value, current_depth))
        return inferred

    # 获取推断类型集合
    inferred_types = parse_and_infer(real_func, 0)

    if not inferred_types:
        return  # 如果函数没有返回值 (返回 None)，忽略

    # ==========================================
    # 第三阶段：将推断出的类型与 allow_data_model 进行严格比对 (校验与拦截)
    # ==========================================
    for g_type in inferred_types:
        # 1. 字典兼容
        if g_type == dict and allow_data_model is dict:
            continue
        # 2. 列表兼容
        if g_type == list and allow_data_model is list:
            continue
        # 3. Pydantic 模型兼容
        if isinstance(g_type, str) and isinstance(allow_data_model, type) and issubclass(allow_data_model, BaseModel):
            if g_type == allow_data_model.__name__:
                continue
        # 4. 无法推断的动态类型 (兜底放行)
        if g_type == Any:
            warnings.warn("AST推断达到最大深度或遇到不可推断的动态类型，已跳过严格校验。")
            continue

        # 5. 如果以上都不满足，说明推断类型与要求的类型不符，触发异常拦截！
        raise TypeMatchError(f"AST 推断输出类型不匹配！推断出的返回格式为 '{g_type}' 语法结构, " f"但要求的 allow_data_model 为 {allow_data_model}")
