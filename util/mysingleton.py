
def singleton(cls):
    """
    create a singleton for target class

    Returns:
        obj: construct from the target class
    """
    instance = {}
    def callfunc(*args, **kwargs):
        if cls not in instance:
            instance[cls] = cls(*args, **kwargs)
        return instance[cls]
    return callfunc


