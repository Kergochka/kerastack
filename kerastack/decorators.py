from typing import Any, Callable, Type, TypeVar, cast

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

def decorator(func: F) -> F:
    """
    Marker meta-decorator for project-level decorators.
    """
    return func

@decorator
def check_columns_for_update(cls: Type[T]) -> Type[T]:
    """
    Check if the class has the required columns for updating.
    """

    from kerastack.KergaSQL import KCoreORM

    if not issubclass(cls, KCoreORM):
        raise TypeError(f"Class {cls.__name__} must inherit from KCoreORM.")

    orm_cls = cast(type[KCoreORM], cls)
    if "_flag_of_cls" not in orm_cls.__dict__:
        orm_cls._flag_of_cls = bytearray(orm_cls._flag_of_cls)
    orm_cls._flag_of_cls[0] |= 1 << 3

    return cast(Type[T], orm_cls)