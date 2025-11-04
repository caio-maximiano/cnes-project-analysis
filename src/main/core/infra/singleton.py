class SingletonMeta(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            inst = super().__call__(*args, **kwargs)
            cls._instances[cls] = inst
        return cls._instances[cls]
